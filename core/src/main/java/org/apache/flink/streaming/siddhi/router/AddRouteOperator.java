/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.siddhi.router;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.siddhi.control.ControlEvent;
import org.apache.flink.streaming.siddhi.control.MetadataControlEvent;
import org.apache.flink.streaming.siddhi.control.OperationControlEvent;
import org.apache.flink.streaming.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.streaming.siddhi.utils.SiddhiExecutionPlanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AddRouteOperator extends AbstractStreamOperator<Tuple2<StreamRoute, Object>>
        implements OneInputStreamOperator<Tuple2<StreamRoute, Object>, Tuple2<StreamRoute, Object>> {

    private Map<String, Set<String>> inputStreamToExecutionPlans = new HashMap<>();

    private Map<String, List<String>> executionPlanIdToPartitionKeys = new HashMap<>();

    private Map<String, Boolean> executionPlanEnabled = new HashMap<>();

    private Map<String, SiddhiStreamSchema<?>> dataStreamSchemas;

    public AddRouteOperator(Map<String, SiddhiStreamSchema<?>> dataStreamSchemas) {
        this.dataStreamSchemas = new HashMap<>(dataStreamSchemas);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<StreamRoute, Object>> element) throws Exception {
        StreamRoute streamRoute = element.getValue().f0;
        Object value = element.getValue().f1;
        if (value instanceof ControlEvent) {
            streamRoute.setBroadCastPartitioning(true);
            if (value instanceof OperationControlEvent) {
                handleOperationControlEvent((OperationControlEvent)value);
            } else if (value instanceof MetadataControlEvent) {
                handleMetadataControlEvent((MetadataControlEvent)value);
            }

            output.collect(element);
        } else {
            String inputStreamId = streamRoute.getInputStreamId();
            if (!inputStreamToExecutionPlans.containsKey(inputStreamId)) {
                return;
            }

            for (String executionPlanId : inputStreamToExecutionPlans.get(inputStreamId)) {
                if (!executionPlanEnabled.get(executionPlanId)) {
                    continue;
                }

                streamRoute.getExecutionPlanIds().clear();

                List<String> partitionKeys = executionPlanIdToPartitionKeys.get(executionPlanId);
                SiddhiStreamSchema<Object> schema = (SiddhiStreamSchema<Object>)dataStreamSchemas.get(inputStreamId);
                String[] fieldNames = schema.getFieldNames();
                Object[] row = schema.getStreamSerializer().getRow(value);
                streamRoute.setPartitionKey(-1);
                for (String partitionKey : partitionKeys) {
                    long partitionValue = 0;
                    for (int i = 0; i < fieldNames.length; ++i) {
                        if (partitionKey.equals(fieldNames[i])) {
                            partitionValue += row[i].hashCode();
                        }
                    }
                    streamRoute.setPartitionKey(Math.abs(partitionValue));
                }

                streamRoute.addExecutionPlanId(executionPlanId);
                output.collect(element);
            }
        }
    }

    private void handleMetadataControlEvent(MetadataControlEvent event) throws Exception {
        if (event.getDeletedExecutionPlanId() != null) {
            for (String executionPlanId : event.getDeletedExecutionPlanId()) {
                for (String inputStreamId : inputStreamToExecutionPlans.keySet()) {
                    inputStreamToExecutionPlans.get(inputStreamId).remove(executionPlanId);
                    if (inputStreamToExecutionPlans.get(inputStreamId).isEmpty()) {
                        inputStreamToExecutionPlans.remove(inputStreamId);
                    }
                }
                executionPlanIdToPartitionKeys.remove(executionPlanId);
                executionPlanEnabled.remove(executionPlanId);
            }
        }

        if (event.getAddedExecutionPlanMap() != null) {
            for (String executionPlanId : event.getAddedExecutionPlanMap().keySet()) {
                for (Set<String> executionPlans : inputStreamToExecutionPlans.values()) {
                    if (executionPlans.contains(executionPlanId)) {
                        throw new Exception("Execution plan " + executionPlanId + " already exists!");
                    }
                }

                String executionPlan = event.getAddedExecutionPlanMap().get(executionPlanId);
                handleExecutionPlan(executionPlanId, executionPlan);
                executionPlanEnabled.put(executionPlanId, true);
            }
        }

        if (event.getUpdatedExecutionPlanMap() != null) {
            for (String executionPlanId : event.getUpdatedExecutionPlanMap().keySet()) {
                if (!executionPlanEnabled.containsKey(executionPlanId)) {
                    throw new Exception("Execution plan " + executionPlanId + " does not exist!");
                }

                String executionPlan = event.getUpdatedExecutionPlanMap().get(executionPlanId);
                handleExecutionPlan(executionPlanId, executionPlan);
            }
        }
    }

    private void handleOperationControlEvent(OperationControlEvent event) throws Exception {
        final OperationControlEvent.Action action = event.getAction();
        if (action == null) {
            throw new Exception("OperationControlEvent.Action is null");
        }
        switch (action) {
            case ENABLE_QUERY:
                // Resume query
                executionPlanEnabled.put(event.getQueryId(), true);
                break;
            case DISABLE_QUERY:
                // Pause query
                executionPlanEnabled.put(event.getQueryId(), false);
                break;
            default:
                throw new IllegalStateException("Illegal action type " + action + ": " + event);
        }
    }

    private void handleExecutionPlan(String executionPlanId, String executionPlan) throws Exception {
        Map<String, SiddhiExecutionPlanner.StreamPartition> streamPartitions =
            SiddhiExecutionPlanner.of(dataStreamSchemas, executionPlan).getStreamPartitions();

        for (String inputStreamId : streamPartitions.keySet()) {
            if (!inputStreamToExecutionPlans.containsKey(inputStreamId)) {
                inputStreamToExecutionPlans.put(inputStreamId, new HashSet<>());
            }

            if (!executionPlanIdToPartitionKeys.containsKey(executionPlanId)) {
                executionPlanIdToPartitionKeys.put(executionPlanId, new ArrayList<>());
            }
            inputStreamToExecutionPlans.get(inputStreamId).add(executionPlanId);

            if(streamPartitions.get(inputStreamId).getPartitonWithList().isEmpty()){
                executionPlanIdToPartitionKeys.get(executionPlanId).addAll(
                        streamPartitions.get(inputStreamId).getGroupByList());
            }else{
                executionPlanIdToPartitionKeys.get(executionPlanId).addAll(
                        streamPartitions.get(inputStreamId).getPartitonWithList());
            }
        }
    }
}