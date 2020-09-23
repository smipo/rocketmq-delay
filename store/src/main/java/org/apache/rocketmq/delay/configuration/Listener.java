package org.apache.rocketmq.delay.configuration;

public interface Listener {
    void onLoad(DynamicConfig config);
}
