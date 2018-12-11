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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.siddhi.control.ControlEvent;
import org.apache.flink.streaming.siddhi.control.MetadataControlEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AddRouteOperator extends AbstractStreamOperator<Tuple2<StreamRoute, Object>>
    implements OneInputStreamOperator<Tuple2<StreamRoute, Object>, Tuple2<StreamRoute, Object>> {
    // input stream id <----> Map<Tuple<partition keys>, execution plan ids>
    // in order to reduce data sent, execution plans with same partition keys will only send one copy of data
    protected Map<String, Map<Tuple, Set<String>>> inputStreamPartitions = new HashMap<>();

    @Override
    public void processElement(StreamRecord<Tuple2<StreamRoute, Object>> element) throws Exception {
        Object value = element.getValue().f1;
        if (value instanceof ControlEvent) {
            // control event
            if (value instanceof MetadataControlEvent) {
                // update partitions
            } else {
                //output.collect();
            }
        } else {
            // think again
            //internalProcessElement(element);
        }
    }

    //protected abstract void internalProcessElement(StreamRecord<Tuple2<StreamRoute, IN>> element) throws Exception;
}
