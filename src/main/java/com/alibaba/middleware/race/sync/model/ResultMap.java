package com.alibaba.middleware.race.sync.model;

/**
 * Created by autulin on 2017/6/17.
 */
public class ResultMap {
    private byte[][][] mapArray;
    private int firstId;

    public ResultMap(int start, int end) {
        this.firstId = start + 1;
        mapArray = new byte[end - firstId][5][];
    }

//    public int size() {
//        return size;
//    }

//    public boolean containsKey(long id) {
//        return mapArray[(int) (id - firstId)] != null;
//    }

    public byte[][] get(long id) {
        int index = (int) (id - firstId);
        if(mapArray[index] == null){
            mapArray[index] = new byte[5][];
        }
        return mapArray[index];
    }

//    public boolean isFull(long id) {
//        int index = (int) (id - firstId);
//        for (byte[] bytes :
//                mapArray[index]) {
//            if (bytes == null) return false;
//        }
//        return true;
//    }

    public void remove(long id) {
        mapArray[(int) (id - firstId)] = null;
    }

//    public void put(long id, byte []data,int i) {
//        byte [][]tmp = mapArray[(int) (id - firstId)];
//        if (tmp == null){
//            tmp = new byte[5][];
//        }
//        tmp[i] = data;
//    }
    public void putArray(long id,byte [][]data){
        mapArray[(int) (id - firstId)] = data;
    }
}
