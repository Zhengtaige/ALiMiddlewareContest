package com.alibaba.middleware.race.sync.model;

/**
 * Created by autulin on 6/9/17.
 */
public class Column {
    public static byte INT_TYPE = 1;
    public static byte STRING_TYPE = 1;

    private String name;
    private byte type;
    private boolean isPrimary;
    private byte[] before;
    private byte[] after;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public byte[] getBefore() {
        return before;
    }

    public void setBefore(byte[] before) {
        this.before = before;
    }

    public Object getAfter() {
        return after;
    }

    public void setAfter(byte[] after) {
        this.after = after;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", isPrimary=" + isPrimary +
                ", before=" + (before == null ? "null" : new String(before)) +
                ", after=" + (after == null ? "null" : new String(after)) +
                '}';
    }
}
