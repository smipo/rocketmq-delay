package org.apache.rocketmq.delay.util;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    protected static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(NamedThreadFactory.class);

    private final AtomicInteger mThreadNum = new AtomicInteger(1);

    private final String mPrefix;

    private final boolean mDaemo;

    private final ThreadGroup mGroup;

    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    public NamedThreadFactory(String prefix, boolean daemo) {
        mPrefix = prefix + "-thread-";
        mDaemo = daemo;
        SecurityManager s = System.getSecurityManager();
        mGroup = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
    }

    public Thread newThread(final Runnable runnable) {
        Runnable safeRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (Exception e) {
                    LOGGER.error("process error", e);
                }
            }
        };
        String name = mPrefix + mThreadNum.getAndIncrement();
        Thread ret = new Thread(mGroup, safeRunnable, name, 0);
        ret.setDaemon(mDaemo);
        return ret;
    }
}
