package org.apache.rocketmq.delay.store.log;

import org.apache.rocketmq.delay.ScheduleIndex;
import org.apache.rocketmq.delay.store.model.LogRecord;
import org.apache.rocketmq.delay.store.model.RecordResult;
import org.apache.rocketmq.delay.store.model.ScheduleRecord;
import org.apache.rocketmq.delay.store.model.ScheduleSetSequence;

import java.util.Map;

public class ScheduleSet extends AbstractDelayLog<ScheduleSetSequence> {

    ScheduleSet(SegmentContainer<RecordResult<ScheduleSetSequence>, LogRecord> container) {
        super(container);
    }

    ScheduleRecord recoverRecord(ScheduleIndex index) {
        return ((ScheduleSetSegmentContainer) container).recover(index.getScheduleTime(), index.getSize(), index.getOffset());
    }

    public void clean() {
        ((ScheduleSetSegmentContainer) container).clean();
    }

    ScheduleSetSegment loadSegment(long segmentBaseOffset) {
        return ((ScheduleSetSegmentContainer) container).loadSegment(segmentBaseOffset);
    }

    synchronized Map<Long, Long> countSegments() {
        return ((ScheduleSetSegmentContainer) container).countSegments();
    }

    void reValidate(final Map<Long, Long> offsets, int singleMessageLimitSize) {
        ((ScheduleSetSegmentContainer) container).reValidate(offsets, singleMessageLimitSize);
    }

    long higherBaseOffset(long low) {
        return ((ScheduleSetSegmentContainer) container).higherBaseOffset(low);
    }
}
