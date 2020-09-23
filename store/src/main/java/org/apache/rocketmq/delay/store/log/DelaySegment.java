package org.apache.rocketmq.delay.store.log;

import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.AppendMessageResult;
import org.apache.rocketmq.delay.store.appender.LogAppender;

import java.io.IOException;

public interface DelaySegment<T> {

    AppendMessageResult<T> append(LogRecord log, LogAppender<T, LogRecord> appender);

    void setWrotePosition(long position);

    long getWrotePosition();

    void setFlushedPosition(long position);

    long getFlushedPosition();

    long getSegmentBaseOffset();

    long validate() throws IOException;

    boolean destroy();

    long flush();
}
