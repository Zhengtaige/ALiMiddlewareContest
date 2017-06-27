package com.alibaba.middleware.race.sync.model;

/**
 * Created by autulin on 2017/6/17.
 */
public class ResultMap {
    public static String[] tableStructure = {"id", "first_name", "last_name", "sex", "score"};
    private byte[][][] mapArray;
    private int firstId;

    public ResultMap(int start, int end) {
        this.firstId = start + 1;
        mapArray = new byte[end - firstId][][];
    }

//    public int size() {
//        return size;
//    }

    public boolean containsKey(long id) {
        return mapArray[(int) (id - firstId)] != null;
    }

    public byte[][] get(long id) {
        int index = (int) (id - firstId);
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

    public void remove(long id) {
        mapArray[(int) (id - firstId)] = null;
    }

    public void put(long id, byte []data,int i) {
        if (mapArray[(int) (id - firstId)] == null){
            mapArray[(int) (id - firstId)] = new byte[5][];
        }
            mapArray[(int) (id - firstId)][i] = data;

    }
    public void putArray(long id,byte [][]data){
        mapArray[(int) (id - firstId)] = data;
    }
}
