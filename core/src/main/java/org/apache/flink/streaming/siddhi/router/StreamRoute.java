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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Each event will carry one {@link StreamRoute},
 * so we will have 1 * n events(n is the different number of {@link StreamRoute}) for each event
 */
public class StreamRoute implements Serializable {
    private String inputStreamId;
    private Set<String> executionPlanIds = new HashSet<>();
    private long partitionKey;
    private boolean broadCastPartitioning = false;

    public StreamRoute() {}

    public StreamRoute(String inputStreamId) {
        this.inputStreamId = inputStreamId;
    }

    public StreamRoute(String inputStreamId, String ... executionPlanIds) {
        this.inputStreamId = inputStreamId;
        this.executionPlanIds = new HashSet<>(Arrays.asList(executionPlanIds));
    }

    public static StreamRoute of(String inputStreamId) {
        return new StreamRoute(inputStreamId);
    }

    public static StreamRoute of(String inputStreamId, String ... executionPlanIds) {
        return new StreamRoute(inputStreamId, executionPlanIds);
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

    public long getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(long partitionKey) {
        this.partitionKey = partitionKey;
    }

    public void addExecutionPlanId(String executionPlanId) {
        this.executionPlanIds.add(executionPlanId);
    }

    public boolean isBroadCastPartitioning() {
        return broadCastPartitioning;
    }

    public void setBroadCastPartitioning(boolean broadCastPartitioning) {
        this.broadCastPartitioning = broadCastPartitioning;
    }
}
