package org.apache.rocketmq.delay.config;


import com.google.common.base.Preconditions;
import org.apache.rocketmq.delay.configuration.DynamicConfig;
import org.apache.rocketmq.delay.util.ConstantsUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class DefaultStoreConfiguration implements StoreConfiguration {
    private static final String SCHEDULE_LOG = "schedule_log";
    private static final String DISPATCH_LOG = "dispatch_log";
    private static final String CHECKPOINT = "checkpoint";
    private static final String PER_MESSAGE_SILE = "per.message.size";
    private static final String LOAD_SEGMENT_DELAY_MIN = "load.segment.delay.min";
    private static final String DISPATCH_LOG_KEEP_HOUR = "dispatch.log.keep.hour";
    private static final String SCHEDULE_CLEAN_BEFORE_DISPATCH_HOUR = "schedule.clean.before.dispatch.hour";
    private static final String LOAD_IN_ADVANCE_MIN = "load.in.advance.min";
    private static final String LOAD_BLOCKING_EXIT_SEC = "load.blocking.exit.sec";
    private static final String SEGMENT_SCALE_MIN = "segment.scale.minute";

    private static final long MS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MS_PER_MINUTE = TimeUnit.MINUTES.toMillis(1);
    private static final long MS_PER_SECONDS = TimeUnit.SECONDS.toMillis(1);
    private static final int SEC_PER_MINUTE = (int) TimeUnit.MINUTES.toSeconds(1);

    private static final int SINGLE_MESSAGE_LIMIT_SIZE = 50 * 1024 * 1024;
    private static final int SEGMENT_LOAD_DELAY_TIMES_IN_MIN = 1;
    private static final int DISPATCH_LOG_KEEP_TIMES_IN_HOUR = 3 * 24;
    private static final int SCHEDULE_CLEAN_BEFORE_DISPATCH_TIMES_IN_HOUR = 24;
    private static final int DEFAULT_SEGMENT_SCALE_IN_MIN = 60;

    private volatile int segmentScale;
    private volatile long inAdvanceLoadMillis;
    private volatile long loadBlockingExitMillis;

    private final DynamicConfig config;

    public DefaultStoreConfiguration(DynamicConfig config) {
        setup(config);
        this.config = config;
    }

    private void setup(DynamicConfig config) {
        int segmentScale = config.getInt(SEGMENT_SCALE_MIN, DEFAULT_SEGMENT_SCALE_IN_MIN);
        validateArguments((segmentScale >= 5) && (segmentScale <= 60), "segment scale in [5, 60] min");
        int inAdvanceLoadMin = config.getInt(LOAD_IN_ADVANCE_MIN, (segmentScale + 1) / 2);
        validateArguments((inAdvanceLoadMin >= 1) && (inAdvanceLoadMin <= ((segmentScale + 1) / 2)), "load in advance time in [1, segmentScale/2] min");
        int loadBlockingExitSec = config.getInt(LOAD_BLOCKING_EXIT_SEC, SEC_PER_MINUTE * (inAdvanceLoadMin + 2) / 3);
        int inAdvanceLoadSec = inAdvanceLoadMin * SEC_PER_MINUTE;
        int loadBlockExitFront = inAdvanceLoadSec / 3;
        int loadBlockExitRear = inAdvanceLoadSec / 2;
        validateArguments((loadBlockingExitSec >= loadBlockExitFront) && (loadBlockingExitSec <= loadBlockExitRear), "load exit block exit time in [inAdvanceLoadMin/3,inAdvanceLoadMin/2] sec. note.");

        this.segmentScale = segmentScale;
        this.inAdvanceLoadMillis = inAdvanceLoadMin * MS_PER_MINUTE;
        this.loadBlockingExitMillis = loadBlockingExitSec * MS_PER_SECONDS;
    }

    private void validateArguments(boolean expression, String errorMessage) {
        Preconditions.checkArgument(expression, errorMessage);
    }

    @Override
    public DynamicConfig getConfig() {
        return config;
    }


    @Override
    public String getScheduleLogStorePath() {
        return buildStorePath(SCHEDULE_LOG);
    }

    @Override
    public String getDispatchLogStorePath() {
        return buildStorePath(DISPATCH_LOG);
    }


    @Override
    public int getSingleMessageLimitSize() {
        return config.getInt(PER_MESSAGE_SILE, SINGLE_MESSAGE_LIMIT_SIZE);
    }

    @Override
    public String getCheckpointStorePath() {
        return buildStorePath(CHECKPOINT);
    }


    @Override
    public long getDispatchLogKeepTime() {
        return config.getInt(DISPATCH_LOG_KEEP_HOUR, DISPATCH_LOG_KEEP_TIMES_IN_HOUR) * MS_PER_HOUR;
    }

    @Override
    public long getCheckCleanTimeBeforeDispatch() {
        return config.getInt(SCHEDULE_CLEAN_BEFORE_DISPATCH_HOUR, SCHEDULE_CLEAN_BEFORE_DISPATCH_TIMES_IN_HOUR) * MS_PER_HOUR;
    }

    @Override
    public long getLogCleanerIntervalSeconds() {
        return config.getInt(ConstantsUtils.LOG_RETENTION_CHECK_INTERVAL_SECONDS, ConstantsUtils.DEFAULT_LOG_RETENTION_CHECK_INTERVAL_SECONDS);
    }

    @Override
    public String getScheduleOffsetCheckpointPath() {
        return buildStorePath(CHECKPOINT);
    }

    @Override
    public long getLoadInAdvanceTimesInMillis() {
        return inAdvanceLoadMillis;
    }

    @Override
    public long getLoadBlockingExitTimesInMillis() {
        return loadBlockingExitMillis;
    }

    @Override
    public boolean isDeleteExpiredLogsEnable() {
        return config.getBoolean(ConstantsUtils.ENABLE_DELETE_EXPIRED_LOGS, false);
    }

    @Override
    public int getSegmentScale() {
        return segmentScale;
    }

    @Override
    public int getLoadSegmentDelayMinutes() {
        return config.getInt(LOAD_SEGMENT_DELAY_MIN, SEGMENT_LOAD_DELAY_TIMES_IN_MIN);
    }

    private String buildStorePath(final String name) {
        final String root = config.getString(ConstantsUtils.STORE_ROOT, ConstantsUtils.LOG_STORE_ROOT);
        return new File(root, name).getAbsolutePath();
    }
}
