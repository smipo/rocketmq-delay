package org.apache.rocketmq.delay.store.appender;


import org.apache.rocketmq.delay.store.model.AppendRecordResult;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.AppendMessageStatus;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class DispatchLogAppender implements LogAppender<Boolean, LogRecord> {
    private final ByteBuffer workingBuffer = ByteBuffer.allocate(Long.BYTES);
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public AppendRecordResult<Boolean> appendLog(LogRecord log) {
        workingBuffer.clear();
        workingBuffer.putLong(log.getStartOffset());
        workingBuffer.flip();

        return new AppendRecordResult<>(AppendMessageStatus.SUCCESS, 0, Long.BYTES, workingBuffer,true);
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
