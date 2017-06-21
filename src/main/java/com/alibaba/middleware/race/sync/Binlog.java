package com.alibaba.middleware.race.sync;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class Binlog {
    private long id = -1;
    private byte[][] data;
    private byte operation;
    private long newid = -1;

    public void setData(byte[][] data) {
        this.data = data;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setOperation(byte operation) {
        this.operation = operation;
    }

    public void setNewid(long newid) {
        this.newid = newid;
    }

    public byte[][] getData() {
        return data;
    }

    public byte getOperation() {
        return operation;
    }

    public long getId() {
        return id;
    }

    public long getNewid() {
        return newid;
    }
}
