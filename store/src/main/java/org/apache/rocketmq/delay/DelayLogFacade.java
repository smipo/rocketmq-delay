package org.apache.rocketmq.delay;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.delay.base.LongHashSet;
import org.apache.rocketmq.delay.store.buffer.SegmentBuffer;
import org.apache.rocketmq.delay.store.log.ScheduleSetSegment;
import org.apache.rocketmq.delay.store.model.AppendLogResult;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.log.DispatchLogSegment;
import org.apache.rocketmq.delay.store.model.ScheduleRecord;
import org.apache.rocketmq.delay.wheel.WheelLoadCursor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

public interface DelayLogFacade {
    void start();

    void shutdown();

    SegmentBuffer getDispatchLogs(long segmentBaseOffset, long dispatchLogOffset);

    long getDispatchLogMaxOffset(long dispatchSegmentBaseOffset);

    DelaySyncRequest.DispatchLogSyncRequest getDispatchLogSyncMaxRequest();

    boolean appendDispatchLogData(long startOffset, long baseOffset, ByteBuffer body);

    List<ScheduleRecord> recoverLogRecord(List<ScheduleIndex> indexList);

    void appendDispatchLog(LogRecord record);

    DispatchLogSegment latestDispatchSegment();

    DispatchLogSegment lowerDispatchSegment(long latestOffset);

    ScheduleSetSegment loadScheduleLogSegment(long segmentBaseOffset);

    WheelLoadCursor.Cursor loadUnDispatch(ScheduleSetSegment setSegment, LongHashSet dispatchedSet, Consumer<ScheduleIndex> refresh);

    long higherScheduleBaseOffset(long index);

    AppendLogResult<ScheduleIndex> appendScheduleLog(LogRecord event);

    long higherDispatchLogBaseOffset(long segmentBaseOffset);

    void appendScheduleLog(MessageExt msgExt) throws Exception;

    void scheduleLogFlush();

    void dispatchLogFlush();

    void appendSlaveScheduleLog(byte[] bodyData);
}
