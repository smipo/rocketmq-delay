package org.apache.rocketmq.delay.configuration;

public interface DynamicConfig {

    String getString(String name, String defaultValue);

    int getInt(String name);

    int getInt(String name, int defaultValue);

    long getLong(String name);

    long getLong(String name, long defaultValue);

    double getDouble(String name);

    double getDouble(String name, double defaultValue);

    boolean getBoolean(String name, boolean defaultValue);

    boolean exist(String name);
}
