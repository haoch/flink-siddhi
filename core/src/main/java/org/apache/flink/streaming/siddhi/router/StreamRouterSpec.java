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


import org.apache.flink.api.java.tuple.Tuple;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Each event will carry one {@link StreamRouterSpec},
 * so we will have 1 * n events(n is the different number of {@link StreamRouterSpec}) for each event
 */
public class StreamRouterSpec implements Serializable {
    private String inputStreamId;
    private Set<String> executionPlanIds = new HashSet<>();
    private Tuple partitionValues;

    public StreamRouterSpec() {}

    public StreamRouterSpec(String inputStreamId) {
        this.inputStreamId = inputStreamId;
    }

    public StreamRouterSpec(String inputStreamId, String ... executionPlanIds) {
        this.inputStreamId = inputStreamId;
        this.executionPlanIds = new HashSet<>(Arrays.asList(executionPlanIds));
    }

    public static StreamRouterSpec of(String inputStreamId) {
        return new StreamRouterSpec(inputStreamId);
    }

    public static StreamRouterSpec of(String inputStreamId, String ... executionPlanIds) {
        return new StreamRouterSpec(inputStreamId, executionPlanIds);
    }

    public String getInputStreamId() {
        return inputStreamId;
    }

    public void setInputStreamId(String inputStreamId) {
        this.inputStreamId = inputStreamId;
    }

    public Set<String> getExecutionPlanIds() {
        return executionPlanIds;
    }

    public void setExecutionPlanIds(Set<String> executionPlanIds) {
        this.executionPlanIds = executionPlanIds;
    }

    public Tuple getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(Tuple partitionValues) {
        this.partitionValues = partitionValues;
    }

    public void addExecutionPlanId(String executionPlanId) {
        this.executionPlanIds.add(executionPlanId);
    }
}
