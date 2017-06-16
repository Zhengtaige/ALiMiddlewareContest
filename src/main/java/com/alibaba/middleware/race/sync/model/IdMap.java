package com.alibaba.middleware.race.sync.model;

/**
 * Created by autulin on 2017/6/17.
 */
public class IdMap {
    private int size;
    private long[] mapArray;
    private int firstId;

    public IdMap(int start, int end) {
        this.firstId = start + 1;
        size = end - firstId; //闭区间
        mapArray = new long[size];
        for (int i = 0; i < size; i++) {
            mapArray[i] = firstId + i;
        }
    }

    public int size() {
        return size;
    }

    public boolean containsKey(long id) {
        return mapArray[(int) (id - firstId)] != -1;
    }

    public long get(long id) {
        return mapArray[(int) (id - firstId)];
    }

    public void remove(long id) {
        mapArray[(int) (id - firstId)] = -1;
        size--;
    }

    public void put(long idBefore, long id) {
        mapArray[(int) (idBefore - firstId)] = id;
    }
}
