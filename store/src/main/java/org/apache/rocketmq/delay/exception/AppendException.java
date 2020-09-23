package org.apache.rocketmq.delay.exception;

public class AppendException extends RuntimeException {

    public AppendException(String message) {
        super(message);
    }
}
