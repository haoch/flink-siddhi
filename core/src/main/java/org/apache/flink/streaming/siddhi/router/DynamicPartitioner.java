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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.siddhi.control.ControlEvent;

import java.util.Random;

public class DynamicPartitioner extends StreamPartitioner<Tuple2<StreamRoute, Object>> {
    private final int[] returnChannels = new int[1];
    private Random random = new Random();
    private Partitioner<Tuple> partitioner;

    public DynamicPartitioner() {
        this.partitioner = new HashPartitioner();
    }

    @Override
    public StreamPartitioner<Tuple2<StreamRoute, Object>> copy() {
        return new DynamicPartitioner();
    }

    @Override
    public int[] selectChannels(SerializationDelegate<StreamRecord<Tuple2<StreamRoute, Object>>> streamRecordSerializationDelegate,
                                int numberOfOutputChannels) {
        Tuple2<StreamRoute, Object> value = streamRecordSerializationDelegate.getInstance().getValue();
        if (value.f1 instanceof ControlEvent) {
            // send to all channels
            int[] channels = new int[numberOfOutputChannels];
            for (int i = 0; i < numberOfOutputChannels; ++i) {
                channels[i] = i;
            }
            return channels;
        }

        if (value.f0.getPartitionValues().getArity() == 0) {
            // random partition
            returnChannels[0] = random.nextInt(numberOfOutputChannels);
        } else {
            returnChannels[0] = partitioner.partition(value.f0.getPartitionValues(), numberOfOutputChannels);
        }
        return returnChannels;
    }
}
