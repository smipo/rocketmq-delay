package org.apache.rocketmq.delay.store.log;


import org.apache.rocketmq.delay.store.PutStatus;
import org.apache.rocketmq.delay.store.model.AppendLogResult;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.model.RecordResult;
import org.apache.rocketmq.delay.util.ConstantsUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class AbstractDelayLog<T> implements Log<RecordResult<T>, LogRecord> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(AbstractDelayLog.class);

    SegmentContainer<RecordResult<T>, LogRecord> container;

    AbstractDelayLog(SegmentContainer<RecordResult<T>, LogRecord> container) {
        this.container = container;
    }

    @Override
    public AppendLogResult<RecordResult<T>> append(LogRecord record) {
        String subject = record.getSubject();
        RecordResult<T> result = container.append(record);
        PutStatus status = result.getStatus();
        if (PutStatus.SUCCESS != status) {
            LOGGER.error("append schedule set file error,subject:{},status:{}", subject, status.name());
            return new AppendLogResult<>(ConstantsUtils.STORE_ERROR, status.name(), null);
        }

        return new AppendLogResult<>(ConstantsUtils.SUCCESS, status.name(), result);
    }

    @Override
    public boolean clean(Long key) {
        return container.clean(key);
    }

    @Override
    public void flush() {
        container.flush();
    }

}
