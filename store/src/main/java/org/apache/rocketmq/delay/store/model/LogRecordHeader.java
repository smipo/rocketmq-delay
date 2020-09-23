package org.apache.rocketmq.delay.store.model;

public class LogRecordHeader {
    private final String subject;
    private final long scheduleTime;

    public LogRecordHeader(String subject,  long scheduleTime) {
        this.subject = subject;
        this.scheduleTime = scheduleTime;
    }

    public String getSubject() {
        return subject;
    }


    public long getScheduleTime() {
        return scheduleTime;
    }


    @Override
    public String toString() {
        return "LogRecordHeader{" +
                "subject=" + subject +
                ", scheduleTime=" + scheduleTime +
                '}';
    }
}
