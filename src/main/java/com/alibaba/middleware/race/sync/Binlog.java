package com.alibaba.middleware.race.sync;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class Binlog {
    private int id = -1;
    private byte[][] data;
    private byte operation;
    private int newid = -1;

    public void setData(byte[][] data) {
        this.data = data;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setOperation(byte operation) {
        this.operation = operation;
    }

    public void setNewid(int newid) {
        this.newid = newid;
    }

    public byte[][] getData() {
        return data;
    }

    public byte getOperation() {
        return operation;
    }

    public int getId() {
        return id;
    }

    public int getNewid() {
        return newid;
    }
}
