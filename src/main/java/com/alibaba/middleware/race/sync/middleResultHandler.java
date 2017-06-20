package com.alibaba.middleware.race.sync;

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
    Map<Character,Map<String,byte[][]>> resultMap = new HashMap<>();
    long t1;

    public middleResultHandler(){
        t1=System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            resultMap.put((char)(i+48),new HashMap<String, byte[][]>());
        }

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

    public void releaseResult() {
        logger.info("[{}]start release result", System.currentTimeMillis());
        FileChannel channel;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(new File(Constants.MIDDLE_HOME + Constants.RESULT_FILE_NAME), "rw");
            channel = randomAccessFile.getChannel();
            for (long i = Server.startPkId + 1; i < Server.endPkId; i++) {
                String id = String.valueOf(i);
                char random = id.charAt(id.length() - 1);
                if (resultMap.containsKey(random)) {
                    Map<String, byte[][]> result = resultMap.get(random);
                    if (result.containsKey(id)) {
                        ByteBuffer byteBuffer = ByteBuffer.allocate(256);
                        byte[][] colomns = result.get(id);

                        byteBuffer.put(id.getBytes()); // id
                        byteBuffer.put((byte) 9); // \t
                        for (int j = 0; j < colomns.length - 1; j++) {
                            byteBuffer.put(colomns[j]);
                            byteBuffer.put((byte) 9); // \t
                        }
                        byteBuffer.put(colomns[colomns.length - 1]);
                        byteBuffer.put((byte) 10); // \n
                        byteBuffer.flip();
//                    System.out.println(byteBuffer.position() + "," + byteBuffer.limit());
//                    System.out.println(new String(byteBuffer.array()));
                        while (byteBuffer.hasRemaining()) {
                            channel.write(byteBuffer);
                        }
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
}
