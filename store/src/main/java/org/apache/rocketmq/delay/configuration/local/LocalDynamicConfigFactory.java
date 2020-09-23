package org.apache.rocketmq.delay.configuration.local;


import org.apache.rocketmq.delay.configuration.DynamicConfig;
import org.apache.rocketmq.delay.configuration.DynamicConfigFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LocalDynamicConfigFactory implements DynamicConfigFactory {
    private final ConfigWatcher watcher = new ConfigWatcher();
    private final ConcurrentMap<String, LocalDynamicConfig> configs = new ConcurrentHashMap<>();

    @Override
    public DynamicConfig create(final String name) {
        if (configs.containsKey(name)) {
            return configs.get(name);
        }

        return doCreate(name);
    }

    private LocalDynamicConfig doCreate(final String name) {
        final LocalDynamicConfig prev = configs.putIfAbsent(name, new LocalDynamicConfig(name));
        final LocalDynamicConfig config = configs.get(name);
        if (prev == null) {
            watcher.addWatch(config);
            config.onConfigModified();
        }
        return config;
    }
}
