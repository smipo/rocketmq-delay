package org.apache.rocketmq.delay.cleaner;

import org.apache.rocketmq.delay.Switchable;
import org.apache.rocketmq.delay.config.StoreConfiguration;
import org.apache.rocketmq.delay.store.log.DispatchLog;
import org.apache.rocketmq.delay.store.log.ScheduleLog;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogCleaner implements Switchable {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LogCleaner.class);

    private final DispatchLog dispatchLog;
    private final ScheduleLog scheduleLog;
    private final StoreConfiguration config;
    private final ScheduledExecutorService cleanScheduler;

    public LogCleaner(StoreConfiguration config, DispatchLog dispatchLog, ScheduleLog scheduleLog) {
        this.config = config;
        this.scheduleLog = scheduleLog;
        this.dispatchLog = dispatchLog;

        this.cleanScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("delay-broker-cleaner-%d").build());
    }

    private void cleanDispatchLog(CleanHook hook) {
        dispatchLog.clean(hook);
    }

    private void cleanScheduleOldLog() {
        scheduleLog.clean();
    }

    private void clean() {
        if (!config.isDeleteExpiredLogsEnable()) return;
        try {
            cleanDispatchLog(scheduleLog::clean);
            cleanScheduleOldLog();
        } catch (Throwable e) {
            LOGGER.error("LogCleaner exec clean error.", e);
        }
    }

    @Override
    public void start() {
        cleanScheduler.scheduleAtFixedRate(this::clean, 0, config.getLogCleanerIntervalSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        cleanScheduler.shutdown();
        try {
            cleanScheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Shutdown log cleaner scheduler interrupted.");
        }
    }

    public interface CleanHook {
        boolean clean(long key);
    }

}
