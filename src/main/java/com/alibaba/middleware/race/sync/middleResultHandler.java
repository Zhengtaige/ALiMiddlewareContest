package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Then on 2017/6/16.
 */
public class middleResultHandler implements Runnable{
    Logger logger = LoggerFactory.getLogger(middleResultHandler.class);
    Map<Character,Map<String,byte[][]>> resultMap = new HashMap<>();
    long t1;
    public middleResultHandler(){
        t1=System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            resultMap.put((char)(i+48),new HashMap<String, byte[][]>());
        }
        logger.info("{}",System.currentTimeMillis()-t1);
    }
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
        String id = binlog.getId();
        resultMap.get(id.charAt(id.length()-1)).remove(id);
    }

    private void insertRow(Binlog binlog){
        String id = binlog.getId();
        resultMap.get(id.charAt(id.length()-1)).put(id,binlog.getData());
    }

    private void updateRow(Binlog binlog){
        String id = binlog.getId();
        Map<String,byte[][]> map = resultMap.get(id.charAt(id.length()-1));
        byte [][]olddata = map.get(id);
        byte [][]updateData = binlog.getData();
        try {
            for (int i = 0; i < updateData.length; i++) {
                if (updateData[i] != null) {
                    olddata[i] = updateData[i];
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        if(binlog.getNewid()!=null){
            map.remove(binlog.getId());
            id = binlog.getNewid();
            map = resultMap.get(id.charAt(id.length()-1));
            map.put(binlog.getNewid(),olddata);
        }
    }
}
