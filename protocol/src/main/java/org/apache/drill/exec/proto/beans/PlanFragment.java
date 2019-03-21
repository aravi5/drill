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
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

public final class PlanFragment implements Externalizable, Message<PlanFragment>, Schema<PlanFragment>
{

    public static Schema<PlanFragment> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static PlanFragment getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final PlanFragment DEFAULT_INSTANCE = new PlanFragment();

    static final long DEFAULT_MEM_INITIAL = 20000000l;
    static final long DEFAULT_MEM_MAX = 2000000000l;
    
    private FragmentHandle handle;
    private float networkCost;
    private float cpuCost;
    private float diskCost;
    private float memoryCost;
    private String fragmentJson;
    private Boolean leafFragment;
    private DrillbitEndpoint assignment;
    private DrillbitEndpoint foreman;
    private long memInitial = DEFAULT_MEM_INITIAL;
    private long memMax = DEFAULT_MEM_MAX;
    private UserCredentials credentials;
    private String optionsJson;
    private QueryContextInformation context;
    private List<Collector> collector;
    private String endpointUUID;

    public PlanFragment()
    {
        
    }

    // getters and setters

    // handle

    public FragmentHandle getHandle()
    {
        return handle;
    }

    public PlanFragment setHandle(FragmentHandle handle)
    {
        this.handle = handle;
        return this;
    }

    // networkCost

    public float getNetworkCost()
    {
        return networkCost;
    }

    public PlanFragment setNetworkCost(float networkCost)
    {
        this.networkCost = networkCost;
        return this;
    }

    // cpuCost

    public float getCpuCost()
    {
        return cpuCost;
    }

    public PlanFragment setCpuCost(float cpuCost)
    {
        this.cpuCost = cpuCost;
        return this;
    }

    // diskCost

    public float getDiskCost()
    {
        return diskCost;
    }

    public PlanFragment setDiskCost(float diskCost)
    {
        this.diskCost = diskCost;
        return this;
    }

    // memoryCost

    public float getMemoryCost()
    {
        return memoryCost;
    }

    public PlanFragment setMemoryCost(float memoryCost)
    {
        this.memoryCost = memoryCost;
        return this;
    }

    // fragmentJson

    public String getFragmentJson()
    {
        return fragmentJson;
    }

    public PlanFragment setFragmentJson(String fragmentJson)
    {
        this.fragmentJson = fragmentJson;
        return this;
    }

    // leafFragment

    public Boolean getLeafFragment()
    {
        return leafFragment;
    }

    public PlanFragment setLeafFragment(Boolean leafFragment)
    {
        this.leafFragment = leafFragment;
        return this;
    }

    // assignment

    public DrillbitEndpoint getAssignment()
    {
        return assignment;
    }

    public PlanFragment setAssignment(DrillbitEndpoint assignment)
    {
        this.assignment = assignment;
        return this;
    }

    // foreman

    public DrillbitEndpoint getForeman()
    {
        return foreman;
    }

    public PlanFragment setForeman(DrillbitEndpoint foreman)
    {
        this.foreman = foreman;
        return this;
    }

    // memInitial

    public long getMemInitial()
    {
        return memInitial;
    }

    public PlanFragment setMemInitial(long memInitial)
    {
        this.memInitial = memInitial;
        return this;
    }

    // memMax

    public long getMemMax()
    {
        return memMax;
    }

    public PlanFragment setMemMax(long memMax)
    {
        this.memMax = memMax;
        return this;
    }

    // credentials

    public UserCredentials getCredentials()
    {
        return credentials;
    }

    public PlanFragment setCredentials(UserCredentials credentials)
    {
        this.credentials = credentials;
        return this;
    }

    // optionsJson

    public String getOptionsJson()
    {
        return optionsJson;
    }

    public PlanFragment setOptionsJson(String optionsJson)
    {
        this.optionsJson = optionsJson;
        return this;
    }

    // context

    public QueryContextInformation getContext()
    {
        return context;
    }

    public PlanFragment setContext(QueryContextInformation context)
    {
        this.context = context;
        return this;
    }

    // collector

    public List<Collector> getCollectorList()
    {
        return collector;
    }

    public PlanFragment setCollectorList(List<Collector> collector)
    {
        this.collector = collector;
        return this;
    }

    // endpointUUID

    public String getEndpointUUID()
    {
        return endpointUUID;
    }

    public PlanFragment setEndpointUUID(String endpointUUID)
    {
        this.endpointUUID = endpointUUID;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<PlanFragment> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public PlanFragment newMessage()
    {
        return new PlanFragment();
    }

    public Class<PlanFragment> typeClass()
    {
        return PlanFragment.class;
    }

    public String messageName()
    {
        return PlanFragment.class.getSimpleName();
    }

    public String messageFullName()
    {
        return PlanFragment.class.getName();
    }

    public boolean isInitialized(PlanFragment message)
    {
        return true;
    }

    public void mergeFrom(Input input, PlanFragment message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.handle = input.mergeObject(message.handle, FragmentHandle.getSchema());
                    break;

                case 4:
                    message.networkCost = input.readFloat();
                    break;
                case 5:
                    message.cpuCost = input.readFloat();
                    break;
                case 6:
                    message.diskCost = input.readFloat();
                    break;
                case 7:
                    message.memoryCost = input.readFloat();
                    break;
                case 8:
                    message.fragmentJson = input.readString();
                    break;
                case 9:
                    message.leafFragment = input.readBool();
                    break;
                case 10:
                    message.assignment = input.mergeObject(message.assignment, DrillbitEndpoint.getSchema());
                    break;

                case 11:
                    message.foreman = input.mergeObject(message.foreman, DrillbitEndpoint.getSchema());
                    break;

                case 12:
                    message.memInitial = input.readInt64();
                    break;
                case 13:
                    message.memMax = input.readInt64();
                    break;
                case 14:
                    message.credentials = input.mergeObject(message.credentials, UserCredentials.getSchema());
                    break;

                case 15:
                    message.optionsJson = input.readString();
                    break;
                case 16:
                    message.context = input.mergeObject(message.context, QueryContextInformation.getSchema());
                    break;

                case 17:
                    if(message.collector == null)
                        message.collector = new ArrayList<Collector>();
                    message.collector.add(input.mergeObject(null, Collector.getSchema()));
                    break;

                case 18:
                    message.endpointUUID = input.readString();
                    break;
                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, PlanFragment message) throws IOException
    {
        if(message.handle != null)
             output.writeObject(1, message.handle, FragmentHandle.getSchema(), false);


        if(message.networkCost != 0)
            output.writeFloat(4, message.networkCost, false);

        if(message.cpuCost != 0)
            output.writeFloat(5, message.cpuCost, false);

        if(message.diskCost != 0)
            output.writeFloat(6, message.diskCost, false);

        if(message.memoryCost != 0)
            output.writeFloat(7, message.memoryCost, false);

        if(message.fragmentJson != null)
            output.writeString(8, message.fragmentJson, false);

        if(message.leafFragment != null)
            output.writeBool(9, message.leafFragment, false);

        if(message.assignment != null)
             output.writeObject(10, message.assignment, DrillbitEndpoint.getSchema(), false);


        if(message.foreman != null)
             output.writeObject(11, message.foreman, DrillbitEndpoint.getSchema(), false);


        if(message.memInitial != DEFAULT_MEM_INITIAL)
            output.writeInt64(12, message.memInitial, false);

        if(message.memMax != DEFAULT_MEM_MAX)
            output.writeInt64(13, message.memMax, false);

        if(message.credentials != null)
             output.writeObject(14, message.credentials, UserCredentials.getSchema(), false);


        if(message.optionsJson != null)
            output.writeString(15, message.optionsJson, false);

        if(message.context != null)
             output.writeObject(16, message.context, QueryContextInformation.getSchema(), false);


        if(message.collector != null)
        {
            for(Collector collector : message.collector)
            {
                if(collector != null)
                    output.writeObject(17, collector, Collector.getSchema(), true);
            }
        }


        if(message.endpointUUID != null)
            output.writeString(18, message.endpointUUID, false);
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "handle";
            case 4: return "networkCost";
            case 5: return "cpuCost";
            case 6: return "diskCost";
            case 7: return "memoryCost";
            case 8: return "fragmentJson";
            case 9: return "leafFragment";
            case 10: return "assignment";
            case 11: return "foreman";
            case 12: return "memInitial";
            case 13: return "memMax";
            case 14: return "credentials";
            case 15: return "optionsJson";
            case 16: return "context";
            case 17: return "collector";
            case 18: return "endpointUUID";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("handle", 1);
        __fieldMap.put("networkCost", 4);
        __fieldMap.put("cpuCost", 5);
        __fieldMap.put("diskCost", 6);
        __fieldMap.put("memoryCost", 7);
        __fieldMap.put("fragmentJson", 8);
        __fieldMap.put("leafFragment", 9);
        __fieldMap.put("assignment", 10);
        __fieldMap.put("foreman", 11);
        __fieldMap.put("memInitial", 12);
        __fieldMap.put("memMax", 13);
        __fieldMap.put("credentials", 14);
        __fieldMap.put("optionsJson", 15);
        __fieldMap.put("context", 16);
        __fieldMap.put("collector", 17);
        __fieldMap.put("endpointUUID", 18);
    }
    
}
