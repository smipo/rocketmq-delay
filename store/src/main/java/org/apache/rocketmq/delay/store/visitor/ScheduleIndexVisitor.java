package org.apache.rocketmq.delay.store.visitor;


import org.apache.rocketmq.delay.ScheduleIndex;
import org.apache.rocketmq.delay.util.CharsetUtils;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

public class ScheduleIndexVisitor extends AbstractLogVisitor<ScheduleIndex> {

    public ScheduleIndexVisitor(long from, FileChannel fileChannel, int singleMessageLimitSize) {
        super(from, fileChannel, singleMessageLimitSize);
    }

    @Override
    protected Optional<ScheduleIndex> readOneRecord(ByteBuffer buffer) {
        long curPos = buffer.position();

        if (buffer.remaining() < Long.BYTES) {
            return Optional.empty();
        }
        int magicCode = buffer.getInt();

        if (buffer.remaining() < Long.BYTES) {
            return Optional.empty();
        }
        long scheduleTime = buffer.getLong();

        if (buffer.remaining() < Long.BYTES) {
            return Optional.empty();
        }
        int subjectLen = buffer.getInt();

        if (buffer.remaining() < subjectLen) {
            return Optional.empty();
        }
        byte[] bs = new byte[subjectLen];
        buffer.get(bs);
        String subject = CharsetUtils.toUTF8String(bs);

        int bodyLen = buffer.getInt();
        if (buffer.remaining() < bodyLen) {
            return Optional.empty();
        }
        byte[] body = new byte[bodyLen];
        buffer.get(body);
        int recordSize = getMetaSize(subjectLen,bodyLen);
        long startOffset = visitedBufferSize();
        buffer.position(Math.toIntExact(curPos + recordSize));

        return Optional.of(new ScheduleIndex(subject, scheduleTime, startOffset, recordSize,body));
    }

    private int getMetaSize(int subjectLen,int bodyLen) {
        return 4 +
                8 +
                4 +
                bodyLen +
                4 +
                subjectLen;
    }
}
