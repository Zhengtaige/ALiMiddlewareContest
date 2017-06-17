package com.alibaba.middleware.race.sync.model;

/**
 * Created by autulin on 2017/6/17.
 */
public class ResultMap {
    public static String[] tableStructure = {"id", "first_name", "last_name", "sex", "score"};
    private int size;
    private byte[][][] mapArray;
    private int firstId;

    public ResultMap(int start, int end) {
        this.firstId = start + 1;
        size = end - firstId; //闭区间
        mapArray = new byte[size][][];
    }

//    public int size() {
//        return size;
//    }

    public boolean containsKey(long id) {
        return mapArray[(int) (id - firstId)] != null;
    }

    public byte[][] get(long id) {
        int index = (int) (id - firstId);
        if (mapArray[index] == null) {
            mapArray[index] = new byte[4][];
        }
        return mapArray[index];
    }

    public boolean isFull(long id) {
        int index = (int) (id - firstId);
        for (byte[] bytes :
                mapArray[index]) {
            if (bytes == null) return false;
        }
        return true;
    }

//    public void remove(long id) {
//        mapArray[(int) (id - firstId)] = -1;
//        size--;
//    }

//    public void put(long idBefore, long id) {
//        mapArray[(int) (idBefore - firstId)] = id;
//    }
}
