package org.apache.rocketmq.delay.store.model;

import org.apache.rocketmq.delay.store.AppendMessageResult;
import org.apache.rocketmq.delay.store.PutStatus;

public class AppendScheduleLogRecordResult implements RecordResult<ScheduleSetSequence> {
    private PutStatus status;

    private AppendMessageResult<ScheduleSetSequence> result;


    public AppendScheduleLogRecordResult(PutStatus status, AppendMessageResult<ScheduleSetSequence> result) {
        this.status = status;
        this.result = result;
    }

    @Override
    public PutStatus getStatus() {
        return status;
    }

    @Override
    public AppendMessageResult<ScheduleSetSequence> getResult() {
        return result;
    }

}
