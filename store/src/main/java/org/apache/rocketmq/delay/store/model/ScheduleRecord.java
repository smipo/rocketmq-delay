package org.apache.rocketmq.delay.store.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ScheduleRecord extends AbstractLogRecord {

    private final LogRecordHeader header;

    private final int bodySize;

    private final ByteBuffer body;

    public ScheduleRecord(String subject, long scheduleTime, ByteBuffer body) {
        this(subject,  scheduleTime, body == null ? 0 : body.capacity(), body);
    }

    public ScheduleRecord(String subject, long scheduleTime, int bodySize, ByteBuffer body) {
        this.header = new LogRecordHeader(subject, scheduleTime);
        this.bodySize = bodySize;
        this.body = body;
    }

    @Override
    public String getSubject() {
        return header.getSubject();
    }


    @Override
    public long getScheduleTime() {
        return header.getScheduleTime();
    }

    @Override
    public int getPayloadSize() {
        return bodySize;
    }

    @Override
    public ByteBuffer getBody() {
        return body;
    }

    @Override
    public int getFullSize() {
        final byte[] subjectBytes = header.getSubject().getBytes(StandardCharsets.UTF_8);
        return 4 +
                8 +
                4 +
                bodySize +
                4 +
                subjectBytes.length;
    }


    @Override
    public String toString() {
        return "ScheduleSetRecord{" +
                "header=" + header +
                ", bodySize=" + bodySize +
                ", body=" + body +
                '}';
    }
}
