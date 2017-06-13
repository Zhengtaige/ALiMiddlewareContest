package com.alibaba.middleware.race.sync;

/**
 * Created by autulin on 6/9/17.
 */
public class Colume {
    public static Short INT_TYPE = 1;
    public static Short STRING_TYPE = 1;

    private String name;
    private byte type;
    private boolean isPrimary;

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

    @Override
    public String toString() {
        return "Colume{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", isPrimary=" + isPrimary +
                '}';
    }
}
