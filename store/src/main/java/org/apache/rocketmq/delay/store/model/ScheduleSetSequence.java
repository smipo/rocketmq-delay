package org.apache.rocketmq.delay.store.model;

public class ScheduleSetSequence {
    private final long scheduleTime;

    public ScheduleSetSequence(long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

}
