package org.apache.rocketmq.delay.configuration.local;

import org.apache.rocketmq.delay.configuration.DynamicConfig;
import org.apache.rocketmq.delay.configuration.DynamicConfigConstant;
import org.apache.rocketmq.delay.configuration.Listener;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalDynamicConfig implements DynamicConfig {

    protected static final InternalLogger LOG = InternalLoggerFactory.getLogger(LocalDynamicConfig.class);

    private final String name;
    private final CopyOnWriteArrayList<Listener> listeners;
    private volatile File file;
    private volatile Map<String, String> config;

    private final String confDir;


    LocalDynamicConfig(String name) {
        this.name = name;
        this.listeners = new CopyOnWriteArrayList<>();
        this.config = new HashMap<>();
        this.confDir = System.getProperty(DynamicConfigConstant.DELAY_PATH);
        this.file = getFileByName(name);
    }

    private File getFileByName(final String name) {
        if (confDir != null && confDir.length() > 0) {
            return new File(confDir, name);
        }
        try {
            final URL res = this.getClass().getClassLoader().getResource(name);
            if (res == null) {
                return null;
            }
            return Paths.get(res.toURI()).toFile();
        } catch (URISyntaxException e) {
            throw new RuntimeException("load config file failed", e);
        }
    }

    long getLastModified() {
        if (file == null) {
            file = getFileByName(name);
        }

        if (file == null) {
            return 0;
        } else {
            return file.lastModified();
        }
    }

    synchronized void onConfigModified() {
        if (file == null) {
            return;
        }

        loadConfig();
        executeListeners();
    }

    private void loadConfig() {
        try {
            final Properties p = new Properties();
            try (Reader reader = new BufferedReader(new FileReader(file))) {
                p.load(reader);
            }
            final Map<String, String> map = new LinkedHashMap<>(p.size());
            for (String key : p.stringPropertyNames()) {
                map.put(key, tryTrim(p.getProperty(key)));
            }

            config = Collections.unmodifiableMap(map);
        } catch (IOException e) {
            LOG.error("load local config failed. config: {}", file.getAbsolutePath(), e);
        }
    }

    private String tryTrim(String data) {
        if (data == null) {
            return null;
        } else {
            return data.trim();
        }
    }

    private void executeListeners() {
        for (Listener listener : listeners) {
            executeListener(listener);
        }
    }

    private void executeListener(Listener listener) {
        try {
            listener.onLoad(this);
        } catch (Throwable e) {
            LOG.error("trigger config listener failed. config: {}", name, e);
        }
    }



    @Override
    public String getString(String name, String defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return value;
    }

    @Override
    public int getInt(String name) {
        return Integer.valueOf(getValueWithCheck(name));
    }

    @Override
    public int getInt(String name, int defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Integer.valueOf(value);
    }

    @Override
    public long getLong(String name) {
        return Long.valueOf(getValueWithCheck(name));
    }

    @Override
    public long getLong(String name, long defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Long.valueOf(value);
    }

    @Override
    public double getDouble(final String name) {
        return Double.valueOf(getValueWithCheck(name));
    }

    @Override
    public double getDouble(final String name, final double defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Double.valueOf(value);
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Boolean.valueOf(value);
    }


    private String getValueWithCheck(String name) {
        String value = getValue(name);
        if (isBlank(value)) {
            throw new RuntimeException("配置项: " + name + " 值为空");
        } else {
            return value;
        }
    }

    private String getValue(String name) {
        return config.get(name);
    }

    private boolean isBlank(final String s) {
        if (s == null || s.isEmpty()) {
            return true;
        }

        for (int i = 0; i < s.length(); i++) {
            if (!Character.isWhitespace(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean exist(String name) {
        return config.containsKey(name);
    }


    @Override
    public String toString() {
        return "LocalDynamicConfig{" +
                "name='" + name + '\'' +
                '}';
    }
}
