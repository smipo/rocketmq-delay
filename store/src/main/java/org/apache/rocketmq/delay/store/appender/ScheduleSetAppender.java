package org.apache.rocketmq.delay.store.appender;

import org.apache.rocketmq.delay.store.model.AppendRecordResult;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.AppendMessageStatus;
import org.apache.rocketmq.delay.store.model.ScheduleSetSequence;
import org.apache.rocketmq.delay.util.ConstantsUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleSetAppender implements LogAppender<ScheduleSetSequence, LogRecord> {

    private final ByteBuffer workingBuffer;
    private final ReentrantLock lock = new ReentrantLock();

    public ScheduleSetAppender(int singleMessageSize) {
        this.workingBuffer = ByteBuffer.allocate(singleMessageSize);
    }

    @Override
    public AppendRecordResult<ScheduleSetSequence> appendLog(LogRecord log) {
        workingBuffer.clear();
        workingBuffer.flip();
        workingBuffer.limit(log.getFullSize());
        workingBuffer.putInt(ConstantsUtils.MESSAGE_MAGIC_CODE);
        long scheduleTime = log.getScheduleTime();
        workingBuffer.putLong(scheduleTime);
        final byte[] subjectBytes = log.getSubject().getBytes(StandardCharsets.UTF_8);
        workingBuffer.putInt(subjectBytes.length);
        workingBuffer.put(subjectBytes);
        workingBuffer.putInt(log.getPayloadSize());
        workingBuffer.put(log.getBody());
        workingBuffer.flip();
        ScheduleSetSequence record = new ScheduleSetSequence(scheduleTime);
        return new AppendRecordResult<>(AppendMessageStatus.SUCCESS, 0, log.getFullSize(), workingBuffer, record);
    }

    @Override
    public void lockAppender() {
        lock.lock();
    }

    @Override
    public void unlockAppender() {
        lock.unlock();
    }
}
