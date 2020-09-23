package org.apache.rocketmq.delay.configuration;

import org.apache.rocketmq.delay.configuration.local.LocalDynamicConfigFactory;

import java.util.ServiceLoader;

public final class DynamicConfigLoader {
    // TODO(keli.wang): can we set this using config?
    private static final DynamicConfigFactory FACTORY;

    static {
        ServiceLoader<DynamicConfigFactory> factories = ServiceLoader.load(DynamicConfigFactory.class);
        DynamicConfigFactory instance = null;
        for (DynamicConfigFactory factory : factories) {
            instance = factory;
            break;
        }

        if (instance == null) {
            instance = new LocalDynamicConfigFactory();
        }

        FACTORY = instance;
    }

    private DynamicConfigLoader() {
    }

    public static DynamicConfig load(final String name) {
        return FACTORY.create(name);
    }
}
