package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.ResultMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Then on 2017/6/16.
 */
public class middleResultHandler implements Runnable{
    public static boolean resultReleased = false;
    Logger logger = LoggerFactory.getLogger(middleResultHandler.class);
    ResultMap resultMap ;

    long t1;

    public middleResultHandler(){
        t1=System.currentTimeMillis();
        resultMap = new ResultMap(Server.startPkId,Server.endPkId);
    }
    @Override
    public void run() {
        try {
            while(true) {
                Binlog binlog = Utils.binlogQueue.take();
                if (binlog.getId() == null) {
                    logger.info("{}","处理中间结果结束!");
                    logger.info("{}",System.currentTimeMillis()-t1);
                    break;
                }else if(!isInRange(Long.valueOf(binlog.getId()))){
                    continue;
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

            releaseResult(); //结束处理，生成结果文件

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void deleteRow(Binlog binlog){
        String id = binlog.getId();
        resultMap.remove(Long.valueOf(id));
    }

    private void insertRow(Binlog binlog){
        String id = binlog.getId();
        resultMap.put(Long.valueOf(id),binlog.getData());
    }

    private void updateRow(Binlog binlog){
        long id = Long.valueOf(binlog.getId());
        byte [][]olddata = resultMap.get(id);
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
            resultMap.remove(id);
            id = Long.valueOf(binlog.getNewid());
            resultMap.put(id,olddata);
        }
    }

    public void releaseResult() {
        logger.info("[{}]start release result", System.currentTimeMillis());
        FileChannel channel;
        try {
            int num = 0 ;
            RandomAccessFile randomAccessFile = new RandomAccessFile(new File(Constants.MIDDLE_HOME + Constants.RESULT_FILE_NAME), "rw");
            channel = randomAccessFile.getChannel();
            for (long i = Server.startPkId + 1; i < Server.endPkId ; i++) {
                byte[][] colomns;
                if ((colomns = resultMap.get(i))!=null) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(256);
                    if(num<10) {
                        String logString = String.valueOf(i) + '\t';
                        byteBuffer.put(String.valueOf(i).getBytes()); // id
                        byteBuffer.put((byte) 9); // \t
                        for (int j = 0; j < colomns.length - 1; j++) {
                            byteBuffer.put(colomns[j]);
                            byteBuffer.put((byte) 9); // \t
                            logString += new String(colomns[j]) + '\t';
                        }
                        byteBuffer.put(colomns[colomns.length - 1]);
                        byteBuffer.put((byte) 10);
                        logString += new String(colomns[colomns.length - 1]);
                        logger.info(logString);
                        num++;
                    }else{
                        byteBuffer.put(String.valueOf(i).getBytes()); // id
                        byteBuffer.put((byte) 9); // \t
                        for (int j = 0; j < colomns.length - 1; j++) {
                            byteBuffer.put(colomns[j]);
                            byteBuffer.put((byte) 9); // \t
                        }
                        byteBuffer.put(colomns[colomns.length - 1]);
                        byteBuffer.put((byte) 10);
                    }
                    byteBuffer.flip();
                    while (byteBuffer.hasRemaining()) {
                        channel.write(byteBuffer);
                    }
                }
            }

            channel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("[{}]release result done.", System.currentTimeMillis());
        resultReleased = true;
    }

    private boolean isInRange(long id){
        if(id>Server.startPkId && id<Server.endPkId){
            return true;
        }else{
            return false;
        }
    }
}
