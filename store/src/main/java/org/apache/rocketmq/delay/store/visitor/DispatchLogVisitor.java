package org.apache.rocketmq.delay.store.visitor;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

public class DispatchLogVisitor extends AbstractLogVisitor<Long> {
    private static final int WORKING_SIZE = 1024 * 1024 * 10;

    public DispatchLogVisitor(long from, FileChannel fileChannel) {
        super(from, fileChannel, WORKING_SIZE);
    }

    @Override
    protected Optional<Long> readOneRecord(ByteBuffer buffer) {
        if (buffer.remaining() < 8) {
            return Optional.empty();
        }
        int startPos = buffer.position();
        long index = buffer.getLong();

        buffer.position(startPos + 8);
        return Optional.of(index);
    }
}
