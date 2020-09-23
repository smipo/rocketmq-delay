package org.apache.rocketmq.delay.store.buffer;

import java.nio.ByteBuffer;

public class SegmentBuffer implements Buffer {
    private final long startOffset;
    private final int size;

    private final ByteBuffer buffer;

    public SegmentBuffer(long startOffset, ByteBuffer buffer, int size) {
        this.startOffset = startOffset;
        this.size = size;
        this.buffer = buffer;
    }

    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getSize() {
        return size;
    }

}
