package org.apache.rocketmq.delay.store.log;

import org.apache.rocketmq.delay.store.model.AppendLogResult;

public interface Log<R, T> {
    AppendLogResult<R> append(T record);

    boolean clean(Long key);

    void flush();
}
