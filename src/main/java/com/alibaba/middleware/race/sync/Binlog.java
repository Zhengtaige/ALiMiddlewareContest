package com.alibaba.middleware.race.sync;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class Binlog {
    private String id;
    private byte[][] data;
    private byte operation;
    private String newid;

    public void setData(byte[][] data) {
        this.data = data;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setOperation(byte operation) {
        this.operation = operation;
    }

    public void setNewid(String newid) {
        this.newid = newid;
    }

    public byte[][] getData() {
        return data;
    }

    public byte getOperation() {
        return operation;
    }

    public String getId() {
        return id;
    }

    public String getNewid() {
        return newid;
    }
}
