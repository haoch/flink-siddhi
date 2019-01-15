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

package org.apache.flink.streaming.siddhi.control;

import java.io.IOException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

public class ControlEventSchema extends AbstractDeserializationSchema<ControlEvent> {

    private ObjectMapper mapper;

    @Override
    public ControlEvent deserialize(byte[] message) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        final ControlMessage controlMessage =  mapper.readValue(message, ControlMessage.class);
        try {
            return (ControlEvent) mapper.readValue(
                controlMessage.getPayload(), Class.forName(controlMessage.getType()));
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}