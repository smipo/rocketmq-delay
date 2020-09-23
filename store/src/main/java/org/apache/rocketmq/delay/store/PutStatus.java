package org.apache.rocketmq.delay.store;

public enum PutStatus {
    SUCCESS(0),
    CREATE_MAPPED_FILE_FAILED(1),
    UNKNOWN_ERROR(-1);

    private int code;

    PutStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}