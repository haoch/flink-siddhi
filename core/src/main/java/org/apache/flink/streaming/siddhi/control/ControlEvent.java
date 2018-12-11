package org.apache.flink.streaming.siddhi.control;

import org.apache.flink.streaming.siddhi.event.Event;

import java.util.Date;

public abstract class ControlEvent implements Event {
    public static final String DEFAULT_INTERNAL_CONTROL_STREAM = "_internal_control_stream";
    private final Date createdTime = new Date();
    private Date expiredTime;
    private boolean expired;

    public String getName() {
        return this.getClass().getSimpleName();
    }

    public Date getCreatedTime() {
        return createdTime;
    }

    public Date getExpiredTime() {
        return expiredTime;
    }

    public boolean isExpired() {
        return expired;
    }

    public void expire() {
        this.expired = true;
        this.expiredTime = new Date();
    }
}