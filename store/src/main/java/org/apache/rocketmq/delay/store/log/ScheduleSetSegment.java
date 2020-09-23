package org.apache.rocketmq.delay.store.log;

import org.apache.rocketmq.delay.ScheduleIndex;
import org.apache.rocketmq.delay.store.model.ScheduleRecord;
import org.apache.rocketmq.delay.store.model.ScheduleSetSequence;
import org.apache.rocketmq.delay.store.visitor.LogVisitor;
import org.apache.rocketmq.delay.store.visitor.ScheduleIndexVisitor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class ScheduleSetSegment extends AbstractDelaySegment<ScheduleSetSequence> {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(ScheduleSetSegment.class);

    ScheduleSetSegment(File file) throws IOException {
        super(file);
    }

    @Override
    public long validate() throws IOException {
        return fileChannel.size();
    }

    ScheduleRecord recover(long offset, int size) {
        // 交给gc，不能给每个segment分配一个局部buffer
        ByteBuffer result = ByteBuffer.allocateDirect(size);
        try {
            int bytes = fileChannel.read(result, offset);
            if (bytes != size) {
                DirectBufCloser.close(result);
                LOGGER.error("schedule set segment recovered failed,need read more bytes,segment:{},offset:{},size:{}, readBytes:{}, segmentTotalSize:{}", fileName, offset, size, bytes, fileChannel.size());
                return null;
            }
            result.flip();
            result.getInt();//读取MESSAGE_MAGIC_CODE
            long scheduleTime = result.getLong();
            int subjectSize = result.getInt();
            byte[] subject = new byte[subjectSize];
            result.get(subject);
            int bodysize = result.getInt();
            byte[] body = new byte[bodysize];
            return new ScheduleRecord(new String(subject, StandardCharsets.UTF_8), scheduleTime,bodysize,result.get(body));
        } catch (Throwable e) {
            LOGGER.error("schedule set segment recovered error,segment:{}, offset-size:{} {}", fileName, offset, size, e);
            return null;
        }
    }

    void loadOffset(long scheduleSetWroteOffset) {
        if (getWrotePosition() != scheduleSetWroteOffset) {
            setWrotePosition(scheduleSetWroteOffset);
            setFlushedPosition(scheduleSetWroteOffset);
            LOGGER.warn("schedule set load offset,exist invalid message,segment base offset:{}, wroteOffset:{}", getSegmentBaseOffset(), scheduleSetWroteOffset);
        }
    }

    public LogVisitor<ScheduleIndex> newVisitor(long from, int singleMessageLimitSize) {
        return new ScheduleIndexVisitor(from, fileChannel, singleMessageLimitSize);
    }

    long doValidate(int singleMessageLimitSize) {
        LOGGER.info("validate schedule log {}", getSegmentBaseOffset());
        LogVisitor<ScheduleIndex> visitor = newVisitor(0, singleMessageLimitSize);
        try {
            while (true) {
                Optional<ScheduleIndex> optionalRecord = visitor.nextRecord();
                if (optionalRecord.isPresent()) {
                    continue;
                }
                break;
            }
            return visitor.visitedBufferSize();
        } finally {
            visitor.close();
        }
    }
}
