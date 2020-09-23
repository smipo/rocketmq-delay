package org.apache.rocketmq.delay;

import org.apache.rocketmq.delay.store.PeriodicFlushService;
import org.apache.rocketmq.delay.store.log.DispatchLog;
import org.apache.rocketmq.delay.store.log.ScheduleLog;

public class LogFlusher implements Switchable {
    private final PeriodicFlushService scheduleLogFlushService;
    private final PeriodicFlushService dispatchLogFlushService;

    LogFlusher(ScheduleLog scheduleLog, DispatchLog dispatchLog) {
        this.dispatchLogFlushService = new PeriodicFlushService(dispatchLog.getProvider());
        this.scheduleLogFlushService = new PeriodicFlushService(scheduleLog.getProvider());
    }

    @Override
    public void start() {
        dispatchLogFlushService.start();
        scheduleLogFlushService.start();
    }

    @Override
    public void shutdown() {
        dispatchLogFlushService.close();
        scheduleLogFlushService.close();
    }

}
