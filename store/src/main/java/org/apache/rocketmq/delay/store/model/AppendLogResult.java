package org.apache.rocketmq.delay.store.model;

public class AppendLogResult<T> {
    private int code;
    private String remark;
    private T additional;

    public AppendLogResult(int code, String remark) {
        this(code, remark, null);
    }

    public AppendLogResult(int code, String remark, T additional) {
        this.code = code;
        this.remark = remark;
        this.additional = additional;
    }

    public int getCode() {
        return code;
    }

    public String getRemark() {
        return remark;
    }

    public T getAdditional() {
        return additional;
    }

}
