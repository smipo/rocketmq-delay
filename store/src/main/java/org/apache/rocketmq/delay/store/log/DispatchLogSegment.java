package org.apache.rocketmq.delay.store.log;

import org.apache.rocketmq.delay.base.SegmentBufferExtend;
import org.apache.rocketmq.delay.store.visitor.DispatchLogVisitor;
import org.apache.rocketmq.delay.store.visitor.LogVisitor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DispatchLogSegment extends AbstractDelaySegment<Boolean> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(DispatchLogSegment.class);

    DispatchLogSegment(File file) throws IOException {
        super(file);
    }

    @Override
    public long validate() throws IOException {
        long size = this.fileChannel.size();
        long invalidateBytes = size % Long.BYTES;
        return size - invalidateBytes;
    }

    public LogVisitor<Long> newVisitor(long from) {
        return new DispatchLogVisitor(from, fileChannel);
    }

    SegmentBufferExtend selectSegmentBuffer(long offset) {
        long wrotePosition = getWrotePosition();
        if (wrotePosition == 0) {
            return new SegmentBufferExtend(0, null, 0, getSegmentBaseOffset());
        }

        if (offset < wrotePosition && offset >= 0) {
            int size = (int) (wrotePosition - offset);
            final ByteBuffer buffer = ByteBuffer.allocate(size);
            try {
                int bytes = fileChannel.read(buffer, offset);
                if (bytes < size) {
                    LOGGER.error("select dispatch log incomplete data to log segment,{}-{}-{}, {} -> {}", getSegmentBaseOffset(), wrotePosition, offset, bytes, size);
                    return null;
                }

                buffer.flip();
                buffer.limit(bytes);
                return new SegmentBufferExtend(offset, buffer, bytes, getSegmentBaseOffset());
            } catch (Throwable e) {
                LOGGER.error("select dispatch log data to log segment failed.", e);
            }
        }

        return null;
    }

    boolean appendData(long startOffset, ByteBuffer body) {
        long currentPos = getWrotePosition();
        int size = body.limit();
        if (startOffset != currentPos) {
            return false;
        }

        try {
            fileChannel.position(currentPos);
            fileChannel.write(body);
        } catch (Throwable e) {
            LOGGER.error("appendData data to log segment failed.", e);
            return false;
        }

        setWrotePosition(currentPos + size);
        return true;
    }

    void fillPreBlank(long untilWhere) {
        setWrotePosition(untilWhere);
    }

    public int entries() {
        try {
            return (int) (validate() / Long.BYTES);
        } catch (Exception e) {
            return 0;
        }
    }
}
