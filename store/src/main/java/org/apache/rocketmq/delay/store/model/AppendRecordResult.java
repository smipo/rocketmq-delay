package org.apache.rocketmq.delay.store.model;

import org.apache.rocketmq.delay.store.AppendMessageStatus;

import java.nio.ByteBuffer;

public class AppendRecordResult<T> {
    private final AppendMessageStatus status;
    private final long wroteOffset;
    private final int wroteBytes;
    private final ByteBuffer buffer;
    private T additional;

    public AppendRecordResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, ByteBuffer buffer) {
        this(status, wroteOffset, wroteBytes, buffer, null);
    }

    public AppendRecordResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, ByteBuffer buffer, T additional) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.buffer = buffer;
        this.additional = additional;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public T getAdditional() {
        return additional;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

}
