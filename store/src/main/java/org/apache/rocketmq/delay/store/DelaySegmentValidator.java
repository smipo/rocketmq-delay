package org.apache.rocketmq.delay.store;

import org.apache.rocketmq.delay.store.log.DelaySegment;

import java.io.IOException;

public interface DelaySegmentValidator {

    long validate(DelaySegment segment) throws IOException;
}
