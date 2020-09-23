package org.apache.rocketmq.delay.store;

public interface Serde<V> {
    byte[] toBytes(final V value);

    V fromBytes(final byte[] data);
}
