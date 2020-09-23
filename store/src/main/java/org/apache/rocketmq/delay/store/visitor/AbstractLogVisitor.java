package org.apache.rocketmq.delay.store.visitor;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractLogVisitor<T> implements LogVisitor<T> {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(AbstractLogVisitor.class);

    private final FileChannel fileChannel;
    private final AtomicLong visited;
    private final AtomicLong visitedSnapshot;
    private final ByteBuffer buffer;

    AbstractLogVisitor(long from, FileChannel fileChannel, int workingSize) {
        this.fileChannel = fileChannel;
        this.visited = new AtomicLong(from);
        this.visitedSnapshot = new AtomicLong(from);
        buffer = ByteBuffer.allocateDirect(workingSize);
        try {
            fileChannel.read(buffer, visited.get());
            buffer.flip();
        } catch (IOException e) {
            LOGGER.error("load dispatch log visitor error", e);
        }
    }

    @Override
    public Optional<T> nextRecord() {
        int start = buffer.position();
        Optional<T> optional = readOneRecord(buffer);
        int delta = buffer.position() - start;

        if (optional.isPresent()) visited.addAndGet(delta);
        else if (visited.get() > visitedSnapshot.get()) {
            if (reAlloc()) return nextRecord();
        }
        return optional;
    }

    private boolean reAlloc() {
        try {
            buffer.clear();
            int bytes = fileChannel.read(buffer, visited.get());
            if (bytes > 0) {
                buffer.flip();
                visitedSnapshot.addAndGet(visited.get() - visitedSnapshot.get());
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("load visitor nextRecord error", e);
        }

        return false;
    }

    protected abstract Optional<T> readOneRecord(ByteBuffer buffer);

    @Override
    public long visitedBufferSize() {
        return visited.get();
    }

    @Override
    public void close() {
        if (buffer == null) return;
        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        cleaner.clean();
    }
}
