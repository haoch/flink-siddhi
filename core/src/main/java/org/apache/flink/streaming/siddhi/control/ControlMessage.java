package org.apache.flink.streaming.siddhi.control;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonRawValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class ControlMessage{
    private final String type;
    @JsonRawValue
    private final String payload;

    public ControlMessage(ControlEvent event) {
        this.type = event.getClass().getName();
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.payload = mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }
}