package org.apache.rocketmq.delay.store.model;

import org.apache.rocketmq.delay.store.AppendMessageResult;
import org.apache.rocketmq.delay.store.PutStatus;

public class AppendDispatchRecordResult implements RecordResult<Boolean> {
    private PutStatus status;
    private AppendMessageResult<Boolean> result;

    public AppendDispatchRecordResult(PutStatus status, AppendMessageResult<Boolean> result) {
        this.status = status;
        this.result = result;
    }

    @Override
    public PutStatus getStatus() {
        return status;
    }

    @Override
    public AppendMessageResult<Boolean> getResult() {
        return result;
    }

}
