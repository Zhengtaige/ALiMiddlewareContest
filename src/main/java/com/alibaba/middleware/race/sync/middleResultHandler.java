package com.alibaba.middleware.race.sync;

import java.util.HashMap;

/**
 * Created by Then on 2017/6/16.
 */
public class middleResultHandler implements Runnable{
    HashMap<String,byte[]> resultMap = new HashMap<>();
    HashMap<Integer,Integer> columnLengthMap = new HashMap<>();
    public middleResultHandler(){
        columnLengthMap.put(0,3);//姓
        columnLengthMap.put(3,6);//名
        columnLengthMap.put(9,1);//性别
        columnLengthMap.put(10,2);//分数

    }
    @Override
    public void run() {
        try {
            Binlog binlog = Utils.binlogQueue.take();
            switch (binlog.getOperation()){
                case 'I':
                    insertRow(binlog);
                    break;
                case 'U':
                    updateRow(binlog);
                    break;
                case 'D':
                    deleteRow(binlog);
                    break;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void deleteRow(Binlog binlog){
        resultMap.remove(binlog.getId());
    }

    private void insertRow(Binlog binlog){
        resultMap.put(binlog.getId(),binlog.getData());
    }

    private void updateRow(Binlog binlog){
        byte []row = resultMap.get(binlog.getId());
        byte []updateData = binlog.getData();
        for (int i = 0; i < updateData.length; i++) {
            int len = columnLengthMap.get(updateData[i]);
            System.arraycopy(updateData,i+1,row,updateData[i],len);
            i+=len+1;
        }
    }

}
