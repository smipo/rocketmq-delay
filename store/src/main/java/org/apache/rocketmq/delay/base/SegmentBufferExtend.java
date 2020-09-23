package org.apache.rocketmq.delay.base;

import org.apache.rocketmq.delay.store.buffer.SegmentBuffer;

import java.nio.ByteBuffer;

public class SegmentBufferExtend extends SegmentBuffer {
    private long baseOffset;

    public SegmentBufferExtend(long startOffset, ByteBuffer buffer, int size, long baseOffset) {
        super(startOffset, buffer, size);
        this.baseOffset = baseOffset;
    }

    public long getBaseOffset() {
        return baseOffset;
    }
}
