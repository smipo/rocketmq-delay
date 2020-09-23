package org.apache.rocketmq.delay.store;

import org.apache.rocketmq.delay.store.log.DelaySegment;

import java.io.IOException;

public class DefaultDelaySegmentValidator implements DelaySegmentValidator {

    @Override
    public long validate(DelaySegment segment) throws IOException {
        return segment.validate();
    }
}
