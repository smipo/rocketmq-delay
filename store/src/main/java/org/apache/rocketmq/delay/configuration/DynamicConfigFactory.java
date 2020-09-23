package org.apache.rocketmq.delay.configuration;

public interface DynamicConfigFactory {
    DynamicConfig create(String name);
}
