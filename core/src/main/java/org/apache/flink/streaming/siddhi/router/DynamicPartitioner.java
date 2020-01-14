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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Random;

public class DynamicPartitioner extends StreamPartitioner<Tuple2<StreamRoute, Object>> {
    private final int[] returnChannels = new int[1];
    private Random random = new Random();
    private Partitioner<Long> partitioner;

    public DynamicPartitioner() {
        this.partitioner = new HashPartitioner();
    }

    @Override
    public StreamPartitioner<Tuple2<StreamRoute, Object>> copy() {
        return new DynamicPartitioner();
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<Tuple2<StreamRoute, Object>>> streamRecordSerializationDelegate) {
        Tuple2<StreamRoute, Object> value = streamRecordSerializationDelegate.getInstance().getValue();
        if (value.f0.getPartitionKey() == -1) {
            // random partition
            return random.nextInt(numberOfChannels);
        } else {
            return partitioner.partition(value.f0.getPartitionKey(), numberOfChannels);
        }
    }
}
