package org.apache.rocketmq.delay.store.log;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class DirectBufCloser {
    public static void close(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }

        try {
            final Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
            cleaner.clean();
        } catch (Exception ignore) {

        }
    }
}
