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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import java.nio.file.Paths;

@Category(SqlTest.class)
public class TestHashJoinJPPDCorrectness extends ClusterTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestHashJoinJPPDCorrectness.class);

  private final String runtimeFilterOption = "ALTER SESSION SET `" + ExecConstants.HASHJOIN_ENABLE_RUNTIME_FILTER_KEY
    + "` = %s";

  private final String runtimeFilterWaitOption = "ALTER SESSION SET `" + ExecConstants
    .HASHJOIN_RUNTIME_FILTER_WAITING_ENABLE_KEY + "` = %s";

  private final String runtimeFilterWaitTimeOption = "ALTER SESSION SET `" + ExecConstants
    .HASHJOIN_RUNTIME_FILTER_MAX_WAITING_TIME_KEY + "` = %d";

  @BeforeClass
  public static void setUp() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("tpchmulti"));
    // Reduce the slice target so that there are multiple minor fragments with exchange, otherwise RuntimeFilter
    // will not be inserted in the plan
    startCluster(ClusterFixture.builder(dirTestWatcher)
      .clusterSize(2)
      .maxParallelization(1)
      .systemOption(ExecConstants.SLICE_TARGET, 10));
  }

  @After
  public void tearDown() {
    client.resetSession(ExecConstants.HASHJOIN_ENABLE_RUNTIME_FILTER_KEY);
    client.resetSession(ExecConstants.HASHJOIN_RUNTIME_FILTER_WAITING_ENABLE_KEY);
  }

  /**
   * Test to make sure runtime filter is inserted in the plan. This is to ensure that with current cluster setup and
   * system options distributed plan is generated rather than single fragment plan. Since in later case RuntimeFilter
   * will not be inserted.
   * @throws Exception
   */
  @Test
  public void testRuntimeFilterPresentInPlan() throws Exception {
    String sql = "SELECT l.n_name, r.r_name FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "l.n_regionkey = r.r_regionkey";

    // don't use client.alterSession since it's an async request and hence there is no guarantee than runtime filter
    // option will be enabled before sending the actual query
    client.runQueriesAndLog(String.format(runtimeFilterOption, "true"));
    UserProtos.QueryPlanFragments planFragments = client.planQuery(sql);
    assertTrue(planFragments.toString().contains("runtime-filter"));
  }

  /**
   * Verifies that result of a query with join condition doesn't changes with and without Runtime Filter for correctness
   * @throws Exception
   */
  @Test
  public void testHashJoinCorrectnessWithRuntimeFilter() throws Exception {
    String sql = "SELECT l.n_name, r.r_name FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "l.n_regionkey = r.r_regionkey";

    String allTestQueryOptions = runtimeFilterOption + "; " + runtimeFilterWaitOption + "; " +
      runtimeFilterWaitTimeOption;
    client.testBuilder()
      .unOrdered()
      .sqlQuery(sql)
      .optionSettingQueriesForTestQuery(allTestQueryOptions, "true", "true", 6000)
      .sqlBaselineQuery(sql)
      .optionSettingQueriesForBaseline(allTestQueryOptions, "false", "false", 1)
      .go();
  }

  /**
   * Verifies that result of query with join condition and filter condition is same with or without RuntimeFilter. In
   * this case order of join and filter condition is same for both test and baseline query
   * @throws Exception
   */
  @Test
  public void testSameOrderOfJoinAndFilterConditionProduceSameResult() throws Exception {
    String testAndBaselineQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "l.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA'";

    String allTestQueryOptions = runtimeFilterOption + "; " + runtimeFilterWaitOption + "; " +
      runtimeFilterWaitTimeOption;
    client.testBuilder()
      .unOrdered()
      .sqlQuery(testAndBaselineQuery)
      .optionSettingQueriesForTestQuery(allTestQueryOptions, "true", "true", 6000)
      .sqlBaselineQuery(testAndBaselineQuery)
      .optionSettingQueriesForBaseline(allTestQueryOptions, "false", "false", 1)
      .go();
  }

  /**
   * Verifies that result of query with join condition and filter condition is same with or without RuntimeFilter. In
   * this case order of join and filter condition is swapped between test and baseline query. Also the filter
   * condition is second in case of test query
   * @throws Exception
   */
  @Test
  public void testDifferentOrderOfJoinAndFilterCondition_filterConditionSecond() throws Exception {
    String testQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "l.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA'";
    String baselineQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "r.r_name = 'AMERICA' and l.n_regionkey = r.r_regionkey";

    String allTestQueryOptions = runtimeFilterOption + "; " + runtimeFilterWaitOption + "; " +
      runtimeFilterWaitTimeOption;
    client.testBuilder()
      .unOrdered()
      .sqlQuery(testQuery)
      .optionSettingQueriesForTestQuery(allTestQueryOptions, "true", "true", 6000)
      .sqlBaselineQuery(baselineQuery)
      .optionSettingQueriesForBaseline(allTestQueryOptions, "false", "false", 1)
      .go();
  }

  /**
   * Verifies that result of query doesn't change if filter and join condition order is different in test query with
   * RuntimeFilter and baseline query without RuntimeFilter. In this case the filter condition is first condition in
   * test query with RuntimeFilter
   * @throws Exception
   */
  @Test
  public void testDifferentOrderOfJoinAndFilterCondition_filterConditionFirst() throws Exception {
    String testQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "r.r_name = 'AMERICA' and l.n_regionkey = r.r_regionkey";
    String baselineQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "l.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA'";

    String allTestQueryOptions = runtimeFilterOption + "; " + runtimeFilterWaitOption + "; " +
      runtimeFilterWaitTimeOption;
    client.testBuilder()
      .unOrdered()
      .sqlQuery(testQuery)
      .optionSettingQueriesForTestQuery(allTestQueryOptions, "true", "true", 6000)
      .sqlBaselineQuery(baselineQuery)
      .optionSettingQueriesForBaseline(allTestQueryOptions, "false", "false", 1)
      .go();
  }

  /**
   * Verifies that result of query doesn't change if filter and join condition order is different in test query with
   * RuntimeFilter and baseline query with RuntimeFilter. In this case both test and baseline query has RuntimeFilter
   * in it.
   * @throws Exception
   */
  @Test
  public void testDifferentOrderOfJoinAndFilterConditionAndRTF() throws Exception {
    String testQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "l.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA'";
    String baselineQuery = "SELECT count(*) FROM dfs.`tpchmulti/nation` l, dfs.`tpchmulti/region/` r where " +
      "r.r_name = 'AMERICA' and l.n_regionkey = r.r_regionkey";

    String allTestQueryOptions = runtimeFilterOption + "; " + runtimeFilterWaitOption + "; " +
      runtimeFilterWaitTimeOption;
    client.testBuilder()
      .unOrdered()
      .sqlQuery(testQuery)
      .optionSettingQueriesForTestQuery(allTestQueryOptions, "true", "true", 6000)
      .sqlBaselineQuery(baselineQuery)
      .optionSettingQueriesForBaseline(allTestQueryOptions, "true", "true", 6000)
      .go();
  }
}
