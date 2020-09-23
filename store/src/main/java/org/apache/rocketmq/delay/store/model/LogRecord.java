package org.apache.rocketmq.delay.store.model;

import java.nio.ByteBuffer;

public interface LogRecord {

    String getSubject();

    long getScheduleTime();

    int getPayloadSize();

    ByteBuffer getBody();

    long getStartOffset();

    int getFullSize();
}
