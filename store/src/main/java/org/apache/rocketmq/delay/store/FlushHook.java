package org.apache.rocketmq.delay.store;

public interface FlushHook {
    void beforeFlush();
}
