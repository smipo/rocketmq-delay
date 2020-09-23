package org.apache.rocketmq.delay.store.model;

import org.apache.rocketmq.delay.store.AppendMessageResult;
import org.apache.rocketmq.delay.store.PutStatus;

public interface RecordResult<T> {

    PutStatus getStatus();

    AppendMessageResult<T> getResult();
}
