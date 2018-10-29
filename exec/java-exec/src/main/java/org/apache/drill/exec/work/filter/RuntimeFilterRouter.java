/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.filter;

import io.netty.buffer.DrillBuf;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.Consumer;
import org.apache.drill.exec.ops.SendingAccountor;
import org.apache.drill.exec.ops.StatusHandler;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.config.RuntimeFilterPOP;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.Wrapper;
import org.apache.drill.exec.proto.BitData;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages the RuntimeFilter routing information of the pushed down join predicate
 * of the partitioned exchange HashJoin.
 *
 * The working flow of the RuntimeFilter has two kinds: Broadcast case and Partitioned case.
 * The HashJoinRecordBatch is responsible to generate the RuntimeFilter.
 * To Partitioned case:
 * The generated RuntimeFilter will be sent to the Foreman node. The Foreman node receives the RuntimeFilter
 * async, broadcasts them to the Scan nodes's MinorFragment. The RuntimeFilterRecordBatch which is downstream
 * to the Scan node will aggregate all the received RuntimeFilter and will leverage it to filter out the
 * scanned rows to generate the SV2.
 * To Broadcast case:
 * The generated RuntimeFilter will be sent to Scan node's RuntimeFilterRecordBatch directly. The working of the
 * RuntimeFilterRecordBath is the same as the Partitioned one.
 */
public class RuntimeFilterRouter {

  private Wrapper rootWrapper;

  //HashJoin node's major fragment id to its corresponding probe side nodes's endpoints
  private Map<Integer, List<CoordinationProtos.DrillbitEndpoint>> joinMjId2probeScanEps = new HashMap<>();

  //HashJoin node's major fragment id to its corresponding probe side scan node's belonging major fragment id
  private Map<Integer, Integer> joinMjId2ScanMjId = new HashMap<>();

  //HashJoin node's major fragment id to its aggregated RuntimeFilterWritable
  private Map<Integer, RuntimeFilterWritable> joinMjId2AggregatedRF = new ConcurrentHashMap<>();

  private Map<Integer, Integer> joinMjId2rfNumber = new ConcurrentHashMap<>();

  private DrillbitContext drillbitContext;

  private SendingAccountor sendingAccountor = new SendingAccountor();

  private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterRouter.class);

  /**
   * This class maintains context for the runtime join push down's filter management. It
   * does a traversal of the physical operators by leveraging the root wrapper which indirectly
   * holds the global PhysicalOperator tree and contains the minor fragment endpoints.
   *
   * @param workUnit
   * @param drillbitContext
   */
  public RuntimeFilterRouter(QueryWorkUnit workUnit, DrillbitContext drillbitContext) {
    this.rootWrapper = workUnit.getRootWrapper();
    this.drillbitContext = drillbitContext;
  }

  /**
   * This method is to collect the parallel information of the RuntimetimeFilters. Then it generates a RuntimeFilter routing map to
   * record the relationship between the RuntimeFilter producers and consumers.
   */
  public void collectRuntimeFilterParallelAndControlInfo() {
    RuntimeFilterParallelismCollector runtimeFilterParallelismCollector = new RuntimeFilterParallelismCollector();
    rootWrapper.getNode().getRoot().accept(runtimeFilterParallelismCollector, null);
    List<RFHelperHolder> holders = runtimeFilterParallelismCollector.getHolders();

    for (RFHelperHolder holder : holders) {
      List<CoordinationProtos.DrillbitEndpoint> probeSideEndpoints = holder.getProbeSideScanEndpoints();
      int probeSideScanMajorId = holder.getProbeSideScanMajorId();
      int joinNodeMajorId = holder.getJoinMajorId();
      int buildSideRfNumber = holder.getBuildSideRfNumber();
      RuntimeFilterDef runtimeFilterDef = holder.getRuntimeFilterDef();
      boolean sendToForeman = runtimeFilterDef.isSendToForeman();
      if (sendToForeman) {
        //send RuntimeFilter to Foreman
        joinMjId2probeScanEps.put(joinNodeMajorId, probeSideEndpoints);
        joinMjId2ScanMjId.put(joinNodeMajorId, probeSideScanMajorId);
        joinMjId2rfNumber.put(joinNodeMajorId, buildSideRfNumber);
      }
    }
  }


  public void waitForComplete() {
    sendingAccountor.waitForSendComplete();
  }

  /**
   * This method is passively invoked by receiving a runtime filter from the network
   *
   * @param srcRuntimeFilterWritable
   */
  public void register(RuntimeFilterWritable srcRuntimeFilterWritable) {
    BitData.RuntimeFilterBDef runtimeFilterB = srcRuntimeFilterWritable.getRuntimeFilterBDef();
    int joinMajorId = runtimeFilterB.getMajorFragmentId();
    int buildSideRfNumber;
    RuntimeFilterWritable toAggregated;
    synchronized (this) {
      buildSideRfNumber = joinMjId2rfNumber.get(joinMajorId);
      buildSideRfNumber--;
      joinMjId2rfNumber.put(joinMajorId, buildSideRfNumber);
      toAggregated = joinMjId2AggregatedRF.get(joinMajorId);
      if (toAggregated == null) {
        toAggregated = srcRuntimeFilterWritable;
        toAggregated.retainBuffers(1);
      } else {
        toAggregated.aggregate(srcRuntimeFilterWritable);
      }
      joinMjId2AggregatedRF.put(joinMajorId, toAggregated);
    }
    if (buildSideRfNumber == 0) {
      joinMjId2AggregatedRF.remove(joinMajorId);
      route(toAggregated);
    }
  }

  private void route(RuntimeFilterWritable srcRuntimeFilterWritable) {
    BitData.RuntimeFilterBDef runtimeFilterB = srcRuntimeFilterWritable.getRuntimeFilterBDef();
    int joinMajorId = runtimeFilterB.getMajorFragmentId();
    UserBitShared.QueryId queryId = runtimeFilterB.getQueryId();
    List<String> probeFields = runtimeFilterB.getProbeFieldsList();
    List<Integer> sizeInBytes = runtimeFilterB.getBloomFilterSizeInBytesList();
    DrillBuf[] data = srcRuntimeFilterWritable.getData();
    List<CoordinationProtos.DrillbitEndpoint> scanNodeEps = joinMjId2probeScanEps.get(joinMajorId);
    int scanNodeSize = scanNodeEps.size();
    srcRuntimeFilterWritable.retainBuffers(scanNodeSize - 1);
    int scanNodeMjId = joinMjId2ScanMjId.get(joinMajorId);
    for (int minorId = 0; minorId < scanNodeEps.size(); minorId++) {
      BitData.RuntimeFilterBDef.Builder builder = BitData.RuntimeFilterBDef.newBuilder();
      for (String probeField : probeFields) {
        builder.addProbeFields(probeField);
      }
      BitData.RuntimeFilterBDef runtimeFilterBDef = builder.setQueryId(queryId).setMajorFragmentId(scanNodeMjId).setMinorFragmentId(minorId).setToForeman(false).addAllBloomFilterSizeInBytes(sizeInBytes).build();
      RuntimeFilterWritable runtimeFilterWritable = new RuntimeFilterWritable(runtimeFilterBDef, data);
      CoordinationProtos.DrillbitEndpoint drillbitEndpoint = scanNodeEps.get(minorId);

      DataTunnel dataTunnel = drillbitContext.getDataConnectionsPool().getTunnel(drillbitEndpoint);
      Consumer<RpcException> exceptionConsumer = new Consumer<RpcException>() {
        @Override
        public void accept(final RpcException e) {
          logger.warn("fail to broadcast a runtime filter to the probe side scan node", e);
        }

        @Override
        public void interrupt(final InterruptedException e) {
          logger.warn("fail to broadcast a runtime filter to the probe side scan node", e);
        }
      };
      RpcOutcomeListener<GeneralRPCProtos.Ack> statusHandler = new StatusHandler(exceptionConsumer, sendingAccountor);
      AccountingDataTunnel accountingDataTunnel = new AccountingDataTunnel(dataTunnel, sendingAccountor, statusHandler);
      accountingDataTunnel.sendRuntimeFilter(runtimeFilterWritable);
    }
  }

  /**
   * Collect the runtime filter parallelism related information such as join node major/minor fragment id , probe side scan node's
   * major/minor fragment id, probe side node's endpoints.
   */
  protected class RuntimeFilterParallelismCollector extends AbstractPhysicalVisitor<Void, RFHelperHolder, RuntimeException> {

    private List<RFHelperHolder> holders = new ArrayList<>();

    @Override
    public Void visitOp(PhysicalOperator op, RFHelperHolder holder) throws RuntimeException {
      boolean isHashJoinOp = op instanceof HashJoinPOP;
      if (isHashJoinOp) {
        HashJoinPOP hashJoinPOP = (HashJoinPOP) op;
        int hashJoinOpId = hashJoinPOP.getOperatorId();
        RuntimeFilterDef runtimeFilterDef = hashJoinPOP.getRuntimeFilterDef();
        if (runtimeFilterDef != null && runtimeFilterDef.isSendToForeman()) {
          if (holder == null || holder.getJoinOpId() != hashJoinOpId) {
            holder = new RFHelperHolder(hashJoinOpId);
            holders.add(holder);
          }
          holder.setRuntimeFilterDef(runtimeFilterDef);
          long runtimeFilterIdentifier = runtimeFilterDef.getRuntimeFilterIdentifier();
          WrapperOperatorsVisitor operatorsVisitor = new WrapperOperatorsVisitor(hashJoinPOP);
          Wrapper container = findTargetWrapper(rootWrapper, operatorsVisitor);
          if (container == null) {
            throw new IllegalStateException(String.format("No valid Wrapper found for HashJoinPOP with id=%d", hashJoinPOP.getOperatorId()));
          }
          int buildSideRFNumber = container.getAssignedEndpoints().size();
          holder.setBuildSideRfNumber(buildSideRFNumber);
          int majorFragmentId = container.getMajorFragmentId();
          holder.setJoinMajorId(majorFragmentId);
          WrapperRuntimeFilterOperatorsVisitor runtimeFilterOperatorsVisitor = new WrapperRuntimeFilterOperatorsVisitor(runtimeFilterIdentifier);
          Wrapper probeSideScanContainer = findTargetWrapper(container, runtimeFilterOperatorsVisitor);
          if (probeSideScanContainer == null) {
            throw new IllegalStateException(String.format("No valid Wrapper found for RuntimeFilterPOP with id=%d", op.getOperatorId()));
          }
          int probeSideScanMjId = probeSideScanContainer.getMajorFragmentId();
          List<CoordinationProtos.DrillbitEndpoint> probeSideScanEps = probeSideScanContainer.getAssignedEndpoints();
          holder.setProbeSideScanEndpoints(probeSideScanEps);
          holder.setProbeSideScanMajorId(probeSideScanMjId);
        }
      }
      return visitChildren(op, holder);
    }

    public List<RFHelperHolder> getHolders() {
      return holders;
    }
  }

  private Wrapper findTargetWrapper(Wrapper wrapper, TargetPhysicalOperatorVisitor targetOpVisitor) {
    targetOpVisitor.setCurrentFragment(wrapper.getNode());
    wrapper.getNode().getRoot().accept(targetOpVisitor, null);
    boolean contain = targetOpVisitor.isContain();
    if (contain) {
      return wrapper;
    }
    List<Wrapper> dependencies = wrapper.getFragmentDependencies();
    if (CollectionUtils.isEmpty(dependencies)) {
      return null;
    }
    for (Wrapper dependencyWrapper : dependencies) {
      Wrapper opContainer = findTargetWrapper(dependencyWrapper, targetOpVisitor);
      if (opContainer != null) {
        return opContainer;
      }
    }
    return null;
  }

  private abstract class TargetPhysicalOperatorVisitor<T, X, E extends Throwable> extends AbstractPhysicalVisitor<T, X, E> {

    protected Exchange sendingExchange;

    public void setCurrentFragment(Fragment fragment) {
      sendingExchange = fragment.getSendingExchange();
    }

    public abstract boolean isContain();
  }

  private class WrapperOperatorsVisitor extends TargetPhysicalOperatorVisitor<Void, Void, RuntimeException> {

    private boolean contain = false;

    private PhysicalOperator targetOp;

    public WrapperOperatorsVisitor(PhysicalOperator targetOp) {
      this.targetOp = targetOp;
    }

    @Override
    public Void visitExchange(Exchange exchange, Void value) throws RuntimeException {
      if (exchange != sendingExchange) {
        return null;
      }
      return exchange.getChild().accept(this, value);
    }

    @Override
    public Void visitOp(PhysicalOperator op, Void value) throws RuntimeException {
      if (op == targetOp) {
        contain = true;
      } else {
        for (PhysicalOperator child : op) {
          child.accept(this, value);
        }
      }
      return null;
    }

    public boolean isContain() {
      return contain;
    }
  }

  private class WrapperRuntimeFilterOperatorsVisitor extends TargetPhysicalOperatorVisitor<Void, Void, RuntimeException> {

    private boolean contain = false;

    private long identifier;


    public WrapperRuntimeFilterOperatorsVisitor(long identifier) {
      this.identifier = identifier;
    }

    @Override
    public Void visitExchange(Exchange exchange, Void value) throws RuntimeException {
      if (exchange != sendingExchange) {
        return null;
      }
      return exchange.getChild().accept(this, value);
    }

    @Override
    public Void visitOp(PhysicalOperator op, Void value) throws RuntimeException {
      boolean same;
      boolean isRuntimeFilterPop = op instanceof RuntimeFilterPOP;
      boolean isHashJoinPop = op instanceof HashJoinPOP;

      if (isHashJoinPop) {
        HashJoinPOP hashJoinPOP = (HashJoinPOP) op;
        PhysicalOperator leftPop = hashJoinPOP.getLeft();
        leftPop.accept(this, value);
        return null;
      }

      if (isRuntimeFilterPop) {
        RuntimeFilterPOP runtimeFilterPOP = (RuntimeFilterPOP) op;
        same = this.identifier == runtimeFilterPOP.getIdentifier();
        if (same) {
          contain = true;
        }
        return null;
      } else {
        for (PhysicalOperator child : op) {
          child.accept(this, value);
        }
      }
      return null;
    }

    public boolean isContain() {
      return contain;
    }
  }

  /**
   * RuntimeFilter helper util holder
   */
  private static class RFHelperHolder {

    private int joinMajorId;

    private int probeSideScanMajorId;

    private List<CoordinationProtos.DrillbitEndpoint> probeSideScanEndpoints;

    private RuntimeFilterDef runtimeFilterDef;

    private int joinOpId;

    private int buildSideRfNumber;

    public RFHelperHolder(int joinOpId) {
      this.joinOpId = joinOpId;
    }

    public int getJoinOpId() {
      return joinOpId;
    }

    public void setJoinOpId(int joinOpId) {
      this.joinOpId = joinOpId;
    }

    public List<CoordinationProtos.DrillbitEndpoint> getProbeSideScanEndpoints() {
      return probeSideScanEndpoints;
    }

    public void setProbeSideScanEndpoints(List<CoordinationProtos.DrillbitEndpoint> probeSideScanEndpoints) {
      this.probeSideScanEndpoints = probeSideScanEndpoints;
    }

    public int getJoinMajorId() {
      return joinMajorId;
    }

    public void setJoinMajorId(int joinMajorId) {
      this.joinMajorId = joinMajorId;
    }

    public int getProbeSideScanMajorId() {
      return probeSideScanMajorId;
    }

    public void setProbeSideScanMajorId(int probeSideScanMajorId) {
      this.probeSideScanMajorId = probeSideScanMajorId;
    }


    public RuntimeFilterDef getRuntimeFilterDef() {
      return runtimeFilterDef;
    }

    public void setRuntimeFilterDef(RuntimeFilterDef runtimeFilterDef) {
      this.runtimeFilterDef = runtimeFilterDef;
    }

    public int getBuildSideRfNumber() {
      return buildSideRfNumber;
    }

    public void setBuildSideRfNumber(int buildSideRfNumber) {
      this.buildSideRfNumber = buildSideRfNumber;
    }
  }
}