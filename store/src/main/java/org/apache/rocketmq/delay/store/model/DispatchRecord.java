package org.apache.rocketmq.delay.store.model;

public class DispatchRecord extends AbstractLogRecord{

    private final LogRecordHeader header;

    private final long startOffset;

    public DispatchRecord(String subject, long scheduleTime,long startOffset){
        this.header = new LogRecordHeader(subject, scheduleTime);
        this.startOffset = startOffset;
    }

    @Override
    public String getSubject() {
        return header.getSubject();
    }


    @Override
    public long getScheduleTime() {
        return header.getScheduleTime();
    }

    @Override
    public long getStartOffset() {
        return startOffset;
    }

}
