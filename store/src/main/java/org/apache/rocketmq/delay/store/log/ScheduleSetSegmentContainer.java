package org.apache.rocketmq.delay.store.log;

import org.apache.rocketmq.delay.config.StoreConfiguration;
import org.apache.rocketmq.delay.store.model.*;
import org.apache.rocketmq.delay.store.AppendMessageResult;
import org.apache.rocketmq.delay.store.DelaySegmentValidator;
import org.apache.rocketmq.delay.store.PutStatus;
import org.apache.rocketmq.delay.store.appender.LogAppender;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ScheduleSetSegmentContainer extends AbstractDelaySegmentContainer<ScheduleSetSequence> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(ScheduleSetSegmentContainer.class);

    private final StoreConfiguration config;

    ScheduleSetSegmentContainer(StoreConfiguration config, File logDir, DelaySegmentValidator validator, LogAppender<ScheduleSetSequence, LogRecord> appender) {
        super(config.getSegmentScale(), logDir, validator, appender);
        this.config = config;
    }

    @Override
    protected void loadLogs(DelaySegmentValidator validator) {
        LOGGER.info("Loading logs.");
        File[] files = this.logDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                if (file.isDirectory()) {
                    continue;
                }

                DelaySegment<ScheduleSetSequence> segment;
                try {
                    segment = new ScheduleSetSegment(file);
                    long size = validator.validate(segment);
                    segment.setWrotePosition(size);
                    segment.setFlushedPosition(size);
                    segments.put(segment.getSegmentBaseOffset(), segment);
                } catch (IOException e) {
                    LOGGER.error("Load {} failed.", file.getAbsolutePath(), e);
                }
            }
        }
        LOGGER.info("Load logs done.");
    }

    @Override
    protected RecordResult<ScheduleSetSequence> retResult(AppendMessageResult<ScheduleSetSequence> result) {
        switch (result.getStatus()) {
            case SUCCESS:
                return new AppendScheduleLogRecordResult(PutStatus.SUCCESS, result);
            default:
                return new AppendScheduleLogRecordResult(PutStatus.UNKNOWN_ERROR, result);
        }
    }

    @Override
    protected DelaySegment<ScheduleSetSequence> allocSegment(long segmentBaseOffset) {
        File nextSegmentFile = new File(logDir, String.valueOf(segmentBaseOffset));
        try {
            DelaySegment<ScheduleSetSequence> logSegment = new ScheduleSetSegment(nextSegmentFile);
            segments.put(segmentBaseOffset, logSegment);
            LOGGER.info("alloc new schedule set segment file {}", ((ScheduleSetSegment) logSegment).fileName);
            return logSegment;
        } catch (IOException e) {
            LOGGER.error("Failed create new schedule set segment file. file: {}", nextSegmentFile.getAbsolutePath(), e);
        }
        return null;
    }

    ScheduleRecord recover(long scheduleTime, int size, long offset) {
        ScheduleSetSegment segment = (ScheduleSetSegment) locateSegment(scheduleTime);
        if (segment == null) {
            LOGGER.error("schedule set recover null value, scheduleTime:{}, size:{}, offset:{}", scheduleTime, size, offset);
            return null;
        }

        return segment.recover(offset, size);
    }

    public void clean() {
        long checkTime = ScheduleOffsetResolver.resolveSegment(System.currentTimeMillis() - config.getDispatchLogKeepTime() - config.getCheckCleanTimeBeforeDispatch(), segmentScale);
        for (DelaySegment<ScheduleSetSequence> segment : segments.values()) {
            if (segment.getSegmentBaseOffset() < checkTime) {
                clean(segment.getSegmentBaseOffset());
            }
        }
    }

    ScheduleSetSegment loadSegment(long segmentBaseOffset) {
        return (ScheduleSetSegment) segments.get(segmentBaseOffset);
    }

    Map<Long, Long> countSegments() {
        final Map<Long, Long> offsets = new HashMap<>(segments.size());
        segments.values().forEach(segment -> offsets.put(segment.getSegmentBaseOffset(), segment.getWrotePosition()));
        return offsets;
    }

    void reValidate(final Map<Long, Long> offsets, int singleMessageLimitSize) {
        segments.values().parallelStream().forEach(segment -> {
            Long offset = offsets.get(segment.getSegmentBaseOffset());
            long wrotePosition = segment.getWrotePosition();
            if (null == offset || offset != wrotePosition) {
                offset = doValidate((ScheduleSetSegment) segment, singleMessageLimitSize);
            } else {
                offset = wrotePosition;
            }

            ((ScheduleSetSegment) segment).loadOffset(offset);
        });
    }

    private long doValidate(ScheduleSetSegment segment, int singleMessageLimitSize) {
        return segment.doValidate(singleMessageLimitSize);
    }
}
