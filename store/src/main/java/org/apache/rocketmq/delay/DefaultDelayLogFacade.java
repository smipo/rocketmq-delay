package org.apache.rocketmq.delay;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.delay.config.StoreConfiguration;
import org.apache.rocketmq.delay.exception.AppendException;
import org.apache.rocketmq.delay.base.LongHashSet;
import org.apache.rocketmq.delay.store.buffer.SegmentBuffer;
import org.apache.rocketmq.delay.cleaner.LogCleaner;
import org.apache.rocketmq.delay.store.log.DispatchLog;
import org.apache.rocketmq.delay.store.log.ScheduleSetSegment;
import org.apache.rocketmq.delay.store.model.AppendLogResult;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.log.DispatchLogSegment;
import org.apache.rocketmq.delay.store.log.ScheduleLog;
import org.apache.rocketmq.delay.store.model.ScheduleRecord;
import org.apache.rocketmq.delay.util.ConstantsUtils;
import org.apache.rocketmq.delay.wheel.WheelLoadCursor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultDelayLogFacade implements DelayLogFacade {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(DefaultDelayLogFacade.class);
    private final ScheduleLog scheduleLog;
    private final DispatchLog dispatchLog;
    private final LogFlusher logFlusher;
    private final LogCleaner cleaner;
    private Function<ScheduleIndex, Boolean> func;

    public DefaultDelayLogFacade(final StoreConfiguration config, final Function<ScheduleIndex, Boolean> func) {
        this.scheduleLog = new ScheduleLog(config);
        this.dispatchLog = new DispatchLog(config);
        this.func = func;
        this.logFlusher = new LogFlusher(scheduleLog, dispatchLog);
        this.cleaner = new LogCleaner(config, dispatchLog, scheduleLog);
    }

    @Override
    public void start() {
        logFlusher.start();
        cleaner.start();
    }


    @Override
    public long getDispatchLogMaxOffset(final long dispatchSegmentBaseOffset) {
        return dispatchLog.getMaxOffset(dispatchSegmentBaseOffset);
    }

    @Override
    public DelaySyncRequest.DispatchLogSyncRequest getDispatchLogSyncMaxRequest() {
        return dispatchLog.getSyncMaxRequest();
    }

    @Override
    public boolean appendDispatchLogData(final long startOffset, final long baseOffset, final ByteBuffer body) {
        return dispatchLog.appendData(startOffset, baseOffset, body);
    }

    @Override
    public SegmentBuffer getDispatchLogs(final long segmentBaseOffset, final long dispatchLogOffset) {
        return dispatchLog.getDispatchLogData(segmentBaseOffset, dispatchLogOffset);
    }

    @Override
    public void shutdown() {
        cleaner.shutdown();
        logFlusher.shutdown();
        scheduleLog.destroy();
    }

    @Override
    public List<ScheduleRecord> recoverLogRecord(final List<ScheduleIndex> indexList) {
        return scheduleLog.recoverLogRecord(indexList);
    }

    @Override
    public void appendDispatchLog(LogRecord record) {
        dispatchLog.append(record);
    }

    @Override
    public DispatchLogSegment latestDispatchSegment() {
        return dispatchLog.latestSegment();
    }

    @Override
    public DispatchLogSegment lowerDispatchSegment(final long baseOffset) {
        return dispatchLog.lowerSegment(baseOffset);
    }

    @Override
    public ScheduleSetSegment loadScheduleLogSegment(final long segmentBaseOffset) {
        return scheduleLog.loadSegment(segmentBaseOffset);
    }

    @Override
    public WheelLoadCursor.Cursor loadUnDispatch(final ScheduleSetSegment setSegment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> refresh) {
        return scheduleLog.loadUnDispatch(setSegment, dispatchedSet, refresh);
    }

    @Override
    public long higherScheduleBaseOffset(long index) {
        return scheduleLog.higherBaseOffset(index);
    }

    @Override
    public long higherDispatchLogBaseOffset(long segmentBaseOffset) {
        return dispatchLog.higherBaseOffset(segmentBaseOffset);
    }

    @Override
    public AppendLogResult<ScheduleIndex> appendScheduleLog(LogRecord event) {
        return scheduleLog.append(event);
    }

    @Override
    public void appendScheduleLog(MessageExt msgExt) throws Exception {
        AppendLogResult<ScheduleIndex> result = scheduleLog.appendScheduleLog(msgExt);
        int code = result.getCode();
        if (ConstantsUtils.SUCCESS != code) {
            LOGGER.error("appendScheduleLog schedule log error,log:{} ,code:{}", msgExt.getTopic(), code);
            throw new AppendException("appendScheduleLogError");
        }
        func.apply(result.getAdditional());
    }

    @Override
    public void scheduleLogFlush(){
        scheduleLog.flush();
    }

    @Override
    public void dispatchLogFlush(){
        dispatchLog.flush();
    }

    @Override
    public void appendSlaveScheduleLog(byte[] bodyData){
        scheduleLog.appendSlaveScheduleLog(bodyData,func);
    }
}
