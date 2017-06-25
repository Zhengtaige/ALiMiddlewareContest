package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class PositiveSq {
    static Logger logger = LoggerFactory.getLogger(PositiveSq.class);
    private static Set<String> firstNameSet = new HashSet<>();
    private static Set<String> lastNameSet = new HashSet<>();
    private static Set<String> nameTotal = new HashSet<>();
    private static int max = 0;
    private static int min = 1000;
    private static boolean isIncrease = true;
    private static long lastUpadteAfterID = 0;
    private static long rowNum=0;
    private static long skipLen = -1;
    private static List<long[]> skipLenList = new LinkedList<>();

    private static List<String> updateIdTop50 = new LinkedList<>();
    private static int idUpdated = 0;
    private static int maxScore = 0;
    private static int i = 1;
    private static boolean idChanged = false;
    private static int Length = 55;
    private static byte[][] readdata;
    private static HashMap<Byte, Byte> typemap = new HashMap<Byte, Byte>();   //记录操作类型以及第几列属性
    private static LinkedList<Byte> namelist = new LinkedList<Byte>();
    private static long beforeid;
    private static long afterid ;
    private static byte operation ;
    private static byte[] first = new byte[3];
    private static byte[] readsex = new byte[3];
    private static byte[] male = {-25, -108, -73};
    private static byte[] female = {-27, -91, -77};
    private static byte type;
    private static MiddleResultHandler middleResultHandler;
//    public static void main(String[] args) throws IOException {
//        long t1 = System.currentTimeMillis();
//        initMap();
//        new Thread(new middleResultHandler()).start();
//        positiveread();
////        logger.info("{}", updateidnum);
//        logger.info("{}", System.currentTimeMillis()-t1);
//    }

    public static void testTime(){
        long t1 = System.currentTimeMillis();
        initMap();
        middleResultHandler = new MiddleResultHandler();
//        new Thread(new middleResultHandler()).start();
        positiveread();
        logger.info("{}", System.currentTimeMillis()-t1);


        //logger.info("firstNameSet size: {}", firstNameSet.toString());
        //logger.info("lastNameSet: {}, size: {}", lastNameSet.toString(), lastNameSet.size());
//        logger.info("id update top 50: {}", updateIdTop50.toString());
        //logger.info("totalNameSet num: {}", nameTotal.size());
        //logger.info("max skip size: {}, min skip size: {}", 55 + max, 55 + min);
        //logger.info("update is increase: {}", isIncrease);

    }

    public static void positiveread() {
        for (int i = 1; i <= 10; i++) {
            try {
                FileChannel fileChannel = new RandomAccessFile(Constants.DATA_HOME+"/" + i + ".txt", "r").getChannel();
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                while (true) {
                    //Step1: 读取废字段
                    mappedByteBuffer.position(mappedByteBuffer.position() + Length);
                    handleIUD(mappedByteBuffer);
                }
            } catch (IllegalArgumentException e){
                logger.info("{}",i+"文件读取完毕!");
            }
            catch (Exception e) {
                logger.info("{}", e.getMessage());
//                logger.info(e.getMessage());
            }
        }
        logger.info("共有"+skipLenList.size()+"次前缀长度变化!");
        for (long[] tmp:
             skipLenList) {
            logger.info(tmp[0]+":"+tmp[1]);
        }
        Binlog binlog = new Binlog();
//        Utils.binlogQueue.offer(binlog);
        middleResultHandler.action(binlog);
    }

    private static void handleIUD(MappedByteBuffer mappedByteBuffer) throws IOException {
        rowNum++;
        Binlog binlog = new Binlog();
        int readd = -1;
        while (true) {
            operation=mappedByteBuffer.get();
            readd++;
            //Step2: 读取操作符
            switch (operation) {
                case 'I':
                    if (readd > max) max = readd;
                    if (readd < min) min = readd;
                    readd = 0;
                    if(readd + Length != skipLen){
                        long []tmp = new long[2];
                        tmp[0] = rowNum;
                        tmp[1] = readd + Length;
                        skipLen = tmp[1];
                        skipLenList.add(tmp);
                    }

                    readdata = new byte[5][];
                    mappedByteBuffer.position(mappedByteBuffer.position()+13);
                    afterid = linkid(mappedByteBuffer, namelist);        //读 id

                    if(!Utils.isInRange(afterid)){
                        mappedByteBuffer.position(mappedByteBuffer.position()+99);
                        while(mappedByteBuffer.get()!='\n');
                        return;
                    }

                    mappedByteBuffer.position(mappedByteBuffer.position()+20);
                    first = new byte[3];
                    mappedByteBuffer.get(first);      //读 姓

                    firstNameSet.add(new String(first));

                    readdata[0]=first;
                    mappedByteBuffer.position(mappedByteBuffer.position()+20);
                    readdata[1]=linkname(mappedByteBuffer, namelist);

                    lastNameSet.add(new String(readdata[1]));
//                    lastNameSet.add(new String(new byte[]{readdata[1][0], readdata[1][1], readdata[1][2]}));
//                    if (readdata[1].length > 3) {
//                        lastNameSet.add(new String(new byte[]{readdata[1][3], readdata[1][4], readdata[1][5]}));
//                    }
                    byte[] name = new byte[first.length + readdata[1].length];
                    System.arraycopy(first, 0, name, 0, first.length);
                    System.arraycopy(readdata[1], 0, name, first.length, readdata[1].length);
                    nameTotal.add(new String(name));


                    mappedByteBuffer.position(mappedByteBuffer.position()+13);
                    byte sex = mappedByteBuffer.get();
                    if (sex == -25) {
                        readdata[2] = male;
                    } else {
                        readdata[2] = female;
                    }

                    mappedByteBuffer.position(mappedByteBuffer.position() + 2);


                    mappedByteBuffer.position(mappedByteBuffer.position()+16);
                    readdata[3]=linkscore(mappedByteBuffer, namelist);

                    mappedByteBuffer.position(mappedByteBuffer.position()+16);      //ztg
                    readdata[4]=linkscore(mappedByteBuffer,namelist);
                    mappedByteBuffer.get();

                    int score1 = Integer.valueOf(new String(readdata[3]));
                    int score2 = Integer.valueOf(new String(readdata[4]));
                    if (score1 > maxScore) maxScore = score1;
                    if (score2 > maxScore) maxScore = score2;
                    if (!idChanged) {
                        if (afterid != i++) {
                            logger.info("here id changed:{}", i--);
                            idChanged = true;
                        }
                    }


                    binlog.setId(afterid);
                    binlog.setOperation(operation);
                    binlog.setData(readdata);
//                    Utils.binlogQueue.offer(binlog);
                    middleResultHandler.action(binlog);
                    return;

                case 'U':
                    if (readd > max) max = readd;
                    if (readd < min) min = readd;
                    readd = 0;
                    if(readd + Length != skipLen){
                        long []tmp = new long[2];
                        tmp[0] = rowNum;
                        tmp[1] = readd + Length;
                        skipLen = tmp[1];
                        skipLenList.add(tmp);
                    }

                    readdata = new byte[5][];
                    mappedByteBuffer.position(mappedByteBuffer.position()+8);
                    beforeid = linkid(mappedByteBuffer, namelist);
                    afterid = linkid(mappedByteBuffer, namelist);
                    if (idUpdated++ < 50) {
                        updateIdTop50.add(beforeid + ">" + afterid);
                    }
                    if (isIncrease) {
                        if (afterid >= lastUpadteAfterID) {
                            lastUpadteAfterID = afterid;
                        } else {
                            isIncrease = false;
                        }
                    }

                    if(!Utils.isInRange(beforeid) ){
                        while(mappedByteBuffer.get()!='\n');
                        return;
                    }else if(!Utils.isInRange(afterid)){
                        while (mappedByteBuffer.get() != '\n') {
                            type = typemap.get(mappedByteBuffer.get());  //读到类型
                            if (type == 0) {
                                mappedByteBuffer.position(mappedByteBuffer.position()+21);
                            } else if (type == 1) {
                                mappedByteBuffer.position(mappedByteBuffer.position()+19);
                                while(mappedByteBuffer.get()!='|');
                            } else if (type == 2) {
                                mappedByteBuffer.position(mappedByteBuffer.position()+14);
                            } else {
                                //            mappedByteBuffer.position(mappedByteBuffer.position()+10);         //ztg
                                mappedByteBuffer.position(mappedByteBuffer.position()+3);
                                if(mappedByteBuffer.get()!='2')        //score1
                                {
                                    mappedByteBuffer.position(mappedByteBuffer.position()+7);
                                }
                                else{
                                    mappedByteBuffer.position(mappedByteBuffer.position()+8);
                                }
                                while(mappedByteBuffer.get()!='|');
                            }
                        }
                        binlog.setId(beforeid);
                        binlog.setOperation((byte)'D');
//                        Utils.binlogQueue.offer(binlog);
                        middleResultHandler.action(binlog);
                        return;
                    }
                    while (mappedByteBuffer.get() != '\n') {
                        type = typemap.get(mappedByteBuffer.get());  //读到类型
                        if (type == 0) {
                            mappedByteBuffer.position(mappedByteBuffer.position()+17);
                            first = new byte[3];
                            mappedByteBuffer.get(first);
                            readdata[type]=first;
                            mappedByteBuffer.position(mappedByteBuffer.position()+1);
                        } else if (type == 1) {
                            mappedByteBuffer.position(mappedByteBuffer.position()+15);
                            while(mappedByteBuffer.get()!='|');
                            readdata[type]=linkname(mappedByteBuffer, namelist);
                        } else if (type == 2) {
                            mappedByteBuffer.position(mappedByteBuffer.position()+10);
                            sex = mappedByteBuffer.get();
                            if (sex == -25) {
                                readdata[type] = male;
                            } else {
                                readdata[type] = female;
                            }
                            mappedByteBuffer.position(mappedByteBuffer.position() + 3);
                        } else {
                //            mappedByteBuffer.position(mappedByteBuffer.position()+10);         //ztg
                            mappedByteBuffer.position(mappedByteBuffer.position()+3);
                            if(mappedByteBuffer.get()!='2')        //score1
                            {
                                mappedByteBuffer.position(mappedByteBuffer.position()+5);
                            }
                            else{
                                mappedByteBuffer.position(mappedByteBuffer.position()+6);
                                type++;
                            }
                            while(mappedByteBuffer.get()!='|');
                            readdata[type]=linkscore(mappedByteBuffer, namelist);
                        }
                    }
                    binlog.setId(beforeid);
                    binlog.setOperation(operation);
                    binlog.setData(readdata);
                    if(beforeid!=afterid) {
                        binlog.setNewid(afterid);
                    }
//                    Utils.binlogQueue.offer(binlog);
                    middleResultHandler.action(binlog);
                    return;

                case 'D':
                    if (readd > max) max = readd;
                    if (readd < min) min = readd;
                    readd = 0;
                    if(readd + Length != skipLen){
                        long []tmp = new long[2];
                        tmp[0] = rowNum;
                        tmp[1] = readd + Length;
                        skipLen = tmp[1];
                        skipLenList.add(tmp);
                    }

                    mappedByteBuffer.position(mappedByteBuffer.position()+8);
                    beforeid = linkid(mappedByteBuffer, namelist);

                    if(!Utils.isInRange(beforeid)){
                        mappedByteBuffer.position(mappedByteBuffer.position()+104);
                        while(mappedByteBuffer.get()!='\n');
                        return;
                    }
                    mappedByteBuffer.position(mappedByteBuffer.position()+104);
                    while (mappedByteBuffer.get() != '\n') ;
                    binlog.setId(beforeid);
                    binlog.setOperation(operation);
//                    Utils.binlogQueue.offer(binlog);
                    middleResultHandler.action(binlog);
                    return;
            }
        }
    }
    public static void initMap() {
        typemap.put((byte)'i', (byte)0);
        typemap.put((byte)'a', (byte)1);
        typemap.put((byte)'e', (byte)2);
        typemap.put((byte)'c', (byte)3);

    }

    public static byte[] linkname(MappedByteBuffer mappedByteBuffer,LinkedList<Byte> name){
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else name.add(temp);
        }
        byte[] res = new byte[name.size()];
        for(int i=0;i<name.size();i++)  res[i] = name.get(i);
        name.clear();
        return res;
    }
    public static byte[] linkscore(MappedByteBuffer mappedByteBuffer,LinkedList<Byte> score){
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else score.add(temp);
        }
        byte[] res = new byte[score.size()];
        for(int i=0;i<score.size();i++)  res[i] = score.get(i);
        score.clear();
        return res;
    }
    public static long linkid(MappedByteBuffer mappedByteBuffer,LinkedList<Byte> id){
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else id.add(temp);
        }
        byte[] res = new byte[id.size()];
        for(int i=0;i<id.size();i++)  res[i] = id.get(i);
        String stringid = new String(res);
        id.clear();
        return Long.valueOf(stringid);
    }
}

