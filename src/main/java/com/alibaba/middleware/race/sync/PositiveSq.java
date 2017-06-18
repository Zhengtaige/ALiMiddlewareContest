package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class PositiveSq {
    static Logger logger = LoggerFactory.getLogger(PositiveSq.class);
    private static int Length = 57;
    private static byte[][] readdata;
    private static HashMap<Byte, Byte> typemap = new HashMap<Byte, Byte>();   //记录操作类型以及第几列属性
    private static LinkedList<Byte> namelist = new LinkedList<Byte>();
    private static String beforeid;
    private static String afterid ;
    private static byte operation ;
    private static byte[] first = new byte[3];
    private static byte[] readsex = new byte[3];
    private static byte type;
//    public static void main(String[] args) throws IOException {
//        long t1 = System.currentTimeMillis();
//        initMap();
//        positiveread();
////        System.out.println(System.currentTimeMillis()-t1);
//        logger.info("{}", System.currentTimeMillis()-t1);
//    }

    public static void testTime(){
        long t1 = System.currentTimeMillis();
        initMap();
        positiveread();
//        System.out.println(System.currentTimeMillis()-t1);
        logger.info("{}", System.currentTimeMillis()-t1);
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
            } catch (Exception e) {
                logger.info("{}", e.getMessage());
            }
        }


    }
    private static void handleIUD(MappedByteBuffer mappedByteBuffer) throws IOException {
        while (true) {
            operation=mappedByteBuffer.get();
            //Step2: 读取操作符
            switch (operation) {
                case 'I':
                    readdata = new byte[4][];
                    mappedByteBuffer.position(mappedByteBuffer.position()+13);
                    afterid = linkid(mappedByteBuffer, namelist);        //读 id

                    mappedByteBuffer.position(mappedByteBuffer.position()+20);
                    mappedByteBuffer.get(first);      //读 姓

                    readdata[0]=first;
                    mappedByteBuffer.position(mappedByteBuffer.position()+20);
                    readdata[1]=linkname(mappedByteBuffer, namelist);

                    mappedByteBuffer.position(mappedByteBuffer.position()+13);
                    mappedByteBuffer.get(readsex);
                    readdata[2]=readsex;

                    mappedByteBuffer.position(mappedByteBuffer.position()+16);
                    readdata[3]=linkscore(mappedByteBuffer, namelist);
                    mappedByteBuffer.get(); //吃掉 '\n'
                    return;

                case 'U':
                    readdata = new byte[4][];
                    mappedByteBuffer.position(mappedByteBuffer.position()+8);
                    beforeid = linkid(mappedByteBuffer, namelist);
                    afterid = linkid(mappedByteBuffer, namelist);
                        while (mappedByteBuffer.get() != '\n') {
                            type = typemap.get(mappedByteBuffer.get());  //读到类型
                            if (type == 0) {
                                mappedByteBuffer.position(mappedByteBuffer.position()+17);
                                mappedByteBuffer.get(first);
                                readdata[type]=first;
                                mappedByteBuffer.position(mappedByteBuffer.position()+1);
                            } else if (type == 1) {
                                mappedByteBuffer.position(mappedByteBuffer.position()+15);
                                while(mappedByteBuffer.get()!='|');
                                readdata[type]=linkname(mappedByteBuffer, namelist);
                            } else if (type == 2) {
                                mappedByteBuffer.position(mappedByteBuffer.position()+10);
                                mappedByteBuffer.get(readsex);
                                readdata[type]=readsex;
                                mappedByteBuffer.position(mappedByteBuffer.position()+1);
                            } else {
                                mappedByteBuffer.position(mappedByteBuffer.position()+10);
                                while(mappedByteBuffer.get()!='|');
                                readdata[type]=linkscore(mappedByteBuffer, namelist);
                            }
                    }
                    return;

                case 'D':
                    mappedByteBuffer.position(mappedByteBuffer.position()+8);
                    beforeid = linkid(mappedByteBuffer, namelist);
                    mappedByteBuffer.position(mappedByteBuffer.position()+87);
                    while (mappedByteBuffer.get() != '\n') ;
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
        byte[] res = new byte[6];
        for(int i=0;i<name.size();i++)  res[i] = name.get(i);
        if(name.size() == 3) res[3] = '\t';
        name.clear();
        return res;
    }
    public static byte[] linkscore(MappedByteBuffer mappedByteBuffer,LinkedList<Byte> score){
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else score.add(temp);
        }
        byte[] res = new byte[4];
        for(int i=0;i<score.size();i++)  res[i] = score.get(i);
        if(score.size()<4){
            res[score.size()] = '\t';
        }
        score.clear();
        return res;
    }
    public static String linkid(MappedByteBuffer mappedByteBuffer,LinkedList<Byte> id){
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else id.add(temp);
        }
        byte[] res = new byte[id.size()];
        for(int i=0;i<id.size();i++)  res[i] = id.get(i);
        String stringid = new String(res);
        id.clear();
        return stringid;
    }
}

