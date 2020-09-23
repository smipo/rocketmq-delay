package org.apache.rocketmq.delay.store.visitor;

import java.util.Optional;

public interface LogVisitor<T> {

    Optional<T> nextRecord();

    long visitedBufferSize();

    void close();

}
