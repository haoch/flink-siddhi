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

package org.apache.flink.streaming.siddhi.utils;

import org.apache.commons.collections.ListUtils;
import org.apache.flink.streaming.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.input.handler.Window;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.OutputStream;
import org.wso2.siddhi.query.api.execution.query.selection.OutputAttribute;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.*;
import java.util.stream.Collectors;

public class SiddhiExecutionPlanner {
    private static final Logger LOG = LoggerFactory.getLogger(SiddhiExecutionPlanner.class);

    private String executionPlan;

    private String enrichedExecutionPlan;

    private Map<String, StreamDefinition> inputStreams = new HashMap<>();

    private Map<String, List<OutputAttribute>> outputStreams = new HashMap<>();

    private Map<String, StreamPartition> streamPartitions;

    public SiddhiExecutionPlanner(Map<String, SiddhiStreamSchema<?>> dataStreamSchemas, String executionPlan) {
        this.executionPlan = executionPlan;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, SiddhiStreamSchema<?>> entry : dataStreamSchemas.entrySet()) {
            sb.append(entry.getValue().getStreamDefinitionExpression(entry.getKey()));
        }
                
        int queryStart = executionPlan.toLowerCase().indexOf("@query");
        if (queryStart == -1) {
        	queryStart = executionPlan.toLowerCase().indexOf("from ");
        }
        
        String prefix = executionPlan.substring(0, queryStart);
        String postfix = executionPlan.substring(queryStart);
        
        enrichedExecutionPlan = prefix + "\r\n" + sb.toString() + "\r\n" + postfix;
    }

    public static SiddhiExecutionPlanner of(Map<String, SiddhiStreamSchema<?>> dataStreamSchemas, String executionPlan) {
        return new SiddhiExecutionPlanner(dataStreamSchemas, executionPlan);
    }

    public String getEnrichedExecutionPlan() {
        return enrichedExecutionPlan;
    }

    private void parse() throws Exception {
        SiddhiApp siddhiApp = SiddhiCompiler.parse(enrichedExecutionPlan);
        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {
            if (!(executionElement instanceof Query)) {
               throw new Exception("Unhandled execution element: " + executionElement.toString());
            }

            InputStream inputStream = ((Query) executionElement).getInputStream();
            Selector selector = ((Query) executionElement).getSelector();
            Map<String, SingleInputStream> queryLevelAliasToStreamMapping = new HashMap<>();

            // Inputs stream definitions
            for (String streamId : inputStream.getUniqueStreamIds()) {
                if (!inputStreams.containsKey(streamId)) {
                    StreamDefinition streamDefinition = siddhiApp.getStreamDefinitionMap().get(streamId);
                    inputStreams.put(streamId, streamDefinition);
                }
            }

            // Window Spec and Partition
            if (inputStream instanceof SingleInputStream) {
                retrieveAliasForQuery((SingleInputStream) inputStream, queryLevelAliasToStreamMapping);
                retrievePartition(findStreamPartition((SingleInputStream) inputStream, selector));
            } else {
                if (inputStream instanceof JoinInputStream) {
                    SingleInputStream leftSingleInputStream = (SingleInputStream) ((JoinInputStream) inputStream)
                            .getLeftInputStream();
                    
                    retrieveAliasForQuery(leftSingleInputStream, queryLevelAliasToStreamMapping);
                    retrievePartition(findStreamPartition(leftSingleInputStream, selector));
                    
                    SingleInputStream rightSingleInputStream = (SingleInputStream) ((JoinInputStream) inputStream)
                            .getRightInputStream();
                  
                    retrieveAliasForQuery(rightSingleInputStream, queryLevelAliasToStreamMapping);
                    retrievePartition(findStreamPartition(rightSingleInputStream, selector));
                } else if (inputStream instanceof StateInputStream) {
                    // Group By Spec
                    List<Variable> groupBy = selector.getGroupByList();
                    if (groupBy.size() > 0) {
                        Map<String, List<Variable>> streamGroupBy = new HashMap<>();
                        for (String streamId : inputStream.getUniqueStreamIds()) {
                            streamGroupBy.put(streamId, new ArrayList<>());
                        }

                        for (Variable variable : groupBy) {
                            if (variable.getStreamId() == null) {
                                //stream not set, then should be all streams' same field
                                for (String streamId : inputStream.getUniqueStreamIds()) {
                                    streamGroupBy.get(streamId).add(variable);
                                }
                            } else {
                                String streamId = retrieveStreamId(variable, queryLevelAliasToStreamMapping);
                                if (streamGroupBy.containsKey(streamId)) {
                                    streamGroupBy.get(streamId).add(variable);
                                } else {
                                    throw new Exception(streamId + " is not defined!");
                                }
                            }
                        }

                        for (Map.Entry<String, List<Variable>> entry : streamGroupBy.entrySet()) {
                            if (entry.getValue().size() > 0) {
                                retrievePartition(generatePartition(entry.getKey(), null, Arrays.asList(entry.getValue().toArray(new Variable[entry.getValue().size()]))));
                            }
                        }
                    }
                }
            }

            // Output streams
            OutputStream outputStream = ((Query) executionElement).getOutputStream();
            outputStreams.put(outputStream.getId(), selector.getSelectionList());
        }

        // Set Partitions
        for (String streamId : inputStreams.keySet()) {
            // Use shuffle partition by default
            if (!streamPartitions.containsKey(streamId)) {
                StreamPartition shufflePartition = new StreamPartition(streamId);
                shufflePartition.setType(StreamPartition.Type.SHUFFLE);
                streamPartitions.put(streamId, shufflePartition);
            }
        }
    }

    private String retrieveStreamId(Variable variable, Map<String, SingleInputStream> aliasMap) throws Exception {
        Preconditions.checkNotNull(variable.getStreamId(), "streamId");
        if (inputStreams.containsKey(variable.getStreamId()) && aliasMap.containsKey(variable.getStreamId())) {
            throw new Exception("Duplicated streamId and alias: " + variable.getStreamId());
        } else if (inputStreams.containsKey(variable.getStreamId())) {
            return variable.getStreamId();
        } else if (aliasMap.containsKey(variable.getStreamId())) {
            return aliasMap.get(variable.getStreamId()).getStreamId();
        } else {
            throw new Exception(variable.getStreamId() + " does not exist!");
        }
    }

    private void retrieveAliasForQuery(SingleInputStream inputStream, Map<String, SingleInputStream> aliasStreamMapping) throws Exception {
        if (inputStream.getStreamReferenceId() != null) {
            if (aliasStreamMapping.containsKey(inputStream.getStreamReferenceId())) {
                throw new Exception("Duplicated stream alias " + inputStream.getStreamId() + " -> " + inputStream);
            } else {
                aliasStreamMapping.put(inputStream.getStreamReferenceId(), inputStream);
            }
        }
    }

    private void retrievePartition(StreamPartition partition) throws Exception {
        if (partition == null) {
            return;
        }

        if (!streamPartitions.containsKey(partition.getInputStreamId())) {
            streamPartitions.put(partition.getInputStreamId(), partition);
        } else {
            StreamPartition existingPartition = streamPartitions.get(partition.getInputStreamId());
            if (existingPartition.getType().equals(partition.getType())
                && ListUtils.isEqualList(existingPartition.getGroupByList(), partition.getGroupByList())
                || existingPartition.getType().equals(StreamPartition.Type.SHUFFLE)) {
                streamPartitions.put(partition.getInputStreamId(), partition);
            } else {
                throw new Exception("You have incompatible partitions on stream " + partition.getInputStreamId()
                    + ": [1] " + streamPartitions.get(partition.getInputStreamId()).toString() + " [2] " + partition.toString() + "");
            }
        }
    }

    private StreamPartition findStreamPartition(SingleInputStream inputStream, Selector selector) {
        // Window Spec
        List<Window> windows = new ArrayList<>();
        for (StreamHandler streamHandler : inputStream.getStreamHandlers()) {
            if (streamHandler instanceof Window) {
                windows.add((Window) streamHandler);
            }
        }

        // Group By Spec
        List<Variable> groupBy = selector.getGroupByList();
        if (windows.size() > 0 || groupBy.size() > 0) {
            return generatePartition(inputStream.getStreamId(), windows, groupBy);
        } else {
            return null;
        }
    }

    private StreamPartition generatePartition(String streamId, List<Window> windows, List<Variable> groupBy) {
        StreamPartition partition = new StreamPartition(streamId);

        if (windows != null && windows.size() > 0) {
            //TODO: sort spec
        }

        if (groupBy != null && groupBy.size() > 0) {
            partition.setGroupByList(groupBy.stream().map(Variable::getAttributeName).collect(Collectors.toList()));
            partition.setType(StreamPartition.Type.GROUPBY);
        } else {
            partition.setType(StreamPartition.Type.SHUFFLE);
        }

        return partition;
    }

    public Map<String, StreamPartition> getStreamPartitions() throws Exception {
        if (streamPartitions == null) {
            try {
                streamPartitions = new HashMap<>();
                parse();
            } catch (Exception ex) {
                LOG.error("Got error to parse policy execution plan: \n{}", executionPlan, ex);
                throw ex;
            }
        }

        return streamPartitions;
    }

    public static class StreamPartition {
        public enum Type {
            GROUPBY,
            SHUFFLE
        }

        private String inputStreamId;
        private Type type;
        private List<String> groupByList = new ArrayList<>();

        public StreamPartition(String inputStreamId) {
            this.inputStreamId = inputStreamId;
        }

        public String getInputStreamId() {
            return inputStreamId;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public List<String> getGroupByList() {
            return groupByList;
        }

        public void setGroupByList(List<String> groupByList) {
            this.groupByList = groupByList;
        }
    }
}
