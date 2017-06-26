package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class PositiveSq {
    static Logger logger = LoggerFactory.getLogger(PositiveSq.class);
    private static int Length = 61;
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
    private static int rowNum = 0;
    private static int skipArrayRownum = 0;
    private static int [][]skipArray = {
            {5992840,62},
            {7401907,55},
            {7402214,56},
            {7402525,57},
            {7405930,58},
            {7439618,59},
            {7710947,60},
            {9406242,61},
            {26796796,62},
            {28437242,55},
            {28437397,56},
            {28437551,57},
            {28439937,58},
            {28474496,59},
            {28682751,60},
            {30406135,61},
            {48373350,62},
            {49815688,55},
            {49815851,56},
            {49816014,57},
            {49817797,58},
            {49835727,59},
            {50008023,60},
            {51736755,61},
            {69480155,62},
            {70943858,55},
            {70944021,56},
            {70944184,57},
            {70945977,58},
            {70971548,59},
            {71150443,60},
            {72886772,61},
            {90317715,62},
            {93642976,55},
            {93643139,56},
            {93643302,57},
            {93645074,58},
            {93677012,59},
            {94020564,60}};
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
    }

    public static void positiveread() {
        for (int i = 1; i <= 10; i++) {
            try {
                FileChannel fileChannel = new RandomAccessFile(Constants.DATA_HOME+"/" + i + ".txt", "r").getChannel();
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                while (true) {
                    //Step1: 读取废字段
                    rowNum++;
                    if((skipArrayRownum<39)&&(rowNum == skipArray[skipArrayRownum][0])){
                        Length = skipArray[skipArrayRownum][1];
                        skipArrayRownum++;
                    }
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
//        Binlog binlog = new Binlog();
//        Utils.binlogQueue.offer(binlog);
        middleResultHandler.releaseResult();


    }
    private static void handleIUD(MappedByteBuffer mappedByteBuffer) throws IOException {
        Binlog binlog = new Binlog();
//        while (true) {
            operation=mappedByteBuffer.get();
            //Step2: 读取操作符
            switch (operation) {
                case 'I':
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

                    readdata[0]=first;
                    mappedByteBuffer.position(mappedByteBuffer.position()+20);
                    readdata[1]=linkname(mappedByteBuffer, namelist);

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

                    binlog.setId(afterid);
                    binlog.setOperation(operation);
                    binlog.setData(readdata);
//                    Utils.binlogQueue.offer(binlog);
                    middleResultHandler.action(binlog);
                    return;

                case 'U':
                    readdata = new byte[5][];
                    mappedByteBuffer.position(mappedByteBuffer.position()+8);
                    beforeid = linkid(mappedByteBuffer, namelist);
                    afterid = linkid(mappedByteBuffer, namelist);
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
//        }
    }
    public static void initMap() {
        typemap.put((byte)'i', (byte)0);
        typemap.put((byte)'a', (byte)1);
        typemap.put((byte)'e', (byte)2);
        typemap.put((byte)'c', (byte)3);

    }

    public static byte[] linkname(MappedByteBuffer mappedByteBuffer, LinkedList<Byte> Kname) {
        byte[] name = new byte[6];
        mappedByteBuffer.get(name, 0, 3);
        byte temp = mappedByteBuffer.get();
        if (temp == '|') {
            byte[] newname = new byte[3];
            System.arraycopy(name, 0, newname, 0, 3);
            return newname;
        } else {
            name[3] = temp;
            mappedByteBuffer.get(name, 4, 2);
            mappedByteBuffer.get();
            return name;
        }
//        while(true){
//            byte temp = mappedByteBuffer.get();
//            if(temp == '|') break;
//            else name.add(temp);
//        }
//        byte[] res = new byte[name.size()];
//        for(int i=0;i<name.size();i++)  res[i] = name.get(i);
//        name.clear();
//        return res;
    }

    public static byte[] linkscore(MappedByteBuffer mappedByteBuffer, LinkedList<Byte> Kscore) {
        byte[] score = new byte[6];
        int tol = 0;
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else {
                score[tol] = temp;
                tol++;
            }
        }
        if(tol==6){
            return score;
        }
        byte[] newscore = new byte[tol];
        System.arraycopy(score, 0, newscore, 0, tol);
        return newscore;
//        while(true){
//            byte temp = mappedByteBuffer.get();
//            if(temp == '|') break;
//            else score.add(temp);
//        }
//        byte[] res = new byte[score.size()];
//        for(int i=0;i<score.size();i++)  res[i] = score.get(i);
//        score.clear();
//        return res;
    }

    //    public static long linkid(MappedByteBuffer mappedByteBuffer,LinkedList<Byte> id){
//        while(true){
//            byte temp = mappedByteBuffer.get();
//            if(temp == '|') break;
//            else id.add(temp);
//        }
//        byte[] res = new byte[id.size()];
//        for(int i=0;i<id.size();i++)  res[i] = id.get(i);
//        String stringid = new String(res);
//        id.clear();
//        return Long.valueOf(stringid);
//    }
//    public static long linkid(MappedByteBuffer mappedByteBuffer, LinkedList<Byte> id) {
//        long bitch = 0;
//        byte temp = mappedByteBuffer.get();
//        if (temp == '8' || temp == '9') {
//            while (mappedByteBuffer.get() != '|') ;
//            return bitch;
//        } else bitch = bitch * 10 + (temp - 48);
//        while (true) {
//            temp = mappedByteBuffer.get();
//            if (temp == '|') break;
//            else bitch = bitch * 10 + (temp - 48);
//        }
//        return bitch;
//    }

    public static long linkid(MappedByteBuffer mappedByteBuffer, LinkedList<Byte> id) {    //ztg尝试修改
        long bitch = 0;
        byte temp = mappedByteBuffer.get();
        if (temp == '8' || temp == '9') {
            while (mappedByteBuffer.get() != '|') ;
            return bitch;
        } else bitch = bitch * 10 + (temp - 48);
        while (true) {
            temp = mappedByteBuffer.get();
            if (temp == '|') break;
            else {
                bitch = bitch * 10 + (temp - 48);
                if(bitch>=8000000) return 0;
            }
        }
        return bitch;
    }
//        if (bitch > 1000000)
//            System.out.println(bitch);
//        while(true){
//            byte temp = mappedByteBuffer.get();
//            if(temp == '|') break;
//            else id.add(temp);
//        }
//        byte[] res = new byte[id.size()];
//        for(int i=0;i<id.size();i++)  res[i] = id.get(i);
//        String stringid = new String(res);
//        id.clear();
//        return Long.valueOf(stringid);
}

