package org.apache.rocketmq.delay;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import java.nio.ByteBuffer;

public class ScheduleIndex {

    private static final Interner<String> INTERNER = Interners.newStrongInterner();

    private final String subject;
    private final long scheduleTime;
    private final long offset;
    private final int size;
    private final byte[] body;

    public ScheduleIndex(String subject, long scheduleTime, long offset, int size) {
       this(subject, scheduleTime,  offset,  size,null);
    }

    public ScheduleIndex(String subject, long scheduleTime, long offset, int size,byte[] body) {
        this.subject = INTERNER.intern(subject);
        this.scheduleTime = scheduleTime;
        this.offset = offset;
        this.size = size;
        this.body = body;
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }


    public String getSubject() {
        return subject;
    }

    public byte[] getBody() {
        return body;
    }
}
