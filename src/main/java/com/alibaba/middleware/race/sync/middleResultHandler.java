package com.alibaba.middleware.race.sync;

import java.util.HashMap;

/**
 * Created by Then on 2017/6/16.
 */
public class middleResultHandler implements Runnable{
    HashMap<String,byte[][]> resultMap = new HashMap<>();
    @Override
    public void run() {
        try {
            while(true) {
                Binlog binlog = Utils.binlogQueue.take();
                if (binlog.getId() == null) {
                    System.out.println("处理中间结果结束!");
                    return;
                }
                switch (binlog.getOperation()) {
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
        byte [][]olddata = resultMap.get(binlog.getId());
        byte [][]updateData = binlog.getData();
        for (int i = 0; i < updateData.length; i++) {
            if(updateData[i]==null){
                updateData[i] = olddata[i];
            }
        }
        if(binlog.getNewid()!=null){
            resultMap.remove(binlog.getId());
            resultMap.put(binlog.getNewid(),updateData);
        }
    }
}
