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