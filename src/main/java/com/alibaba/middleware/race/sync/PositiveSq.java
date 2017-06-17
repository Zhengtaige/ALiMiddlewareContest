package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Row;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class PositiveSq {
    private static int Length = "|mysql-bin.000018829057528|1497264609000|middleware5|student|".getBytes().length;
    private static byte[] readdata = new byte[16];
    private static byte[] readupdate = new byte[20];
    private static byte[] wastewords = new byte[Length];
    private static HashMap<Byte, Byte> typemap = new HashMap<Byte, Byte>();   //记录操作类型以及第几列属性
    private static long reflectarea = 0;
    private static LinkedList<Byte> namelist = new LinkedList<Byte>();
    private static String beforeid=null;
    private static String afterid = null;
    private static byte[] operation = new byte[1];
    private static byte[] first = new byte[3];
    private static byte[] last = new byte[6];
    private static byte[] readsex = new byte[3];
    private static byte[] score = new byte[4];
    private static byte[] type = new byte[1];
    public static void main(String[] args) throws IOException {
        long t1 = System.currentTimeMillis();
        initMap();
        positiveread(new File("E:\\__下载\\Data_Ali\\canal.txt"));
        System.out.println(System.currentTimeMillis()-t1);
    }

    public static void positiveread(File file) {
        Long startTime = System.currentTimeMillis();
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file), Utils.Buffer_Size));
            while(true){
                    //Step1: 读取废字段
                     int stop = inputStream.read(wastewords);
                if(!new String(wastewords,0,10).equals("|mysql-bin")){
                    System.out.println("!!!");
                }
                     if(stop == -1) return;
                handleIUD(inputStream);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void handleIUD(DataInputStream inputStream) throws IOException {
        while (inputStream.read(operation)!=-1) {
            //Step2: 读取操作符
            switch (operation[0]) {
                case 'I':
                    passline(inputStream, 3);
                    afterid = linkid(inputStream, namelist);        //读 id
                    passline(inputStream, 2);
                    inputStream.read(first);      //读 姓
                    System.arraycopy(first, 0, readdata, 0, 3);
                    passline(inputStream, 3);
                    System.arraycopy(linkname(inputStream, namelist), 0, readdata, 3, 6);   //读 名
                    passline(inputStream, 2);
                    inputStream.read(readsex);
                    System.arraycopy(readsex, 0, readdata, 9, 3);
                    passline(inputStream, 3);
                    System.arraycopy(linkscore(inputStream, namelist), 0, readdata, 12, 4);   //读 分 数
                    byte tmpb = inputStream.readByte(); //吃掉 '\n'
                    if (tmpb != '\n') {
                        System.out.println("!!");
                    }
                    return;

                case 'U':
                    passline(inputStream, 2);
                    beforeid = linkid(inputStream, namelist);
                    afterid = linkid(inputStream, namelist);
                    int pos = 0;
                        while (inputStream.readByte() != '\n') {
                            type[0] = typemap.get(inputStream.readByte());  //读到类型
                            System.arraycopy(type, 0, readupdate, pos, 1);
                            pos++;
                            passline(inputStream, 2);
                            if (type[0] == 1) {
                                inputStream.read(first);
                                System.arraycopy(first, 0, readupdate, pos, 3);
                                pos += 3;
                                inputStream.readByte();
                            } else if (type[0] == 2) {
                                System.arraycopy(linkname(inputStream, namelist), 0, readupdate, pos, 6);   //读 名
                                pos += 6;
                            } else if (type[0] == 3) {
                                inputStream.read(readsex);
                                System.arraycopy(readsex, 0, readupdate, pos, 3);
                                pos++;
                                inputStream.readByte();
                            } else {
                                System.arraycopy(linkscore(inputStream, namelist), 0, readupdate, pos, 4);   //读 分 数
                                pos += 4;
                            }
                        if (pos < 20) readupdate[pos] = '\t';
                        return;
                    }

                case 'D':
                    passline(inputStream, 2);
                    beforeid = linkid(inputStream, namelist);
                    while (inputStream.readByte() != '\n') ;
                    return;
            }
        }
    }
    public static void initMap() {
        typemap.put((byte)'i', (byte)1);
        typemap.put((byte)'a', (byte)2);
        typemap.put((byte)'e', (byte)3);
        typemap.put((byte)'c', (byte)4);


    }
    public static void passline(DataInputStream dis, int num){
        try {

            while(true){

                    if(dis.readByte()=='|'){
                        num--;
                    }
                    if(num == 0) return;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static byte[] linkname(DataInputStream dis,LinkedList<Byte> name){
        while(true){
            try {
                byte temp = dis.readByte();
                if(temp == '|') break;
                else name.add(temp);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        byte[] res = new byte[6];
        for(int i=0;i<name.size();i++)  res[i] = name.get(i);
        if(name.size() == 3) res[3] = '\t';
        name.clear();
        return res;
    }
    public static byte[] linkscore(DataInputStream dis,LinkedList<Byte> score){
        while(true){
            try {
                byte temp = dis.readByte();
                if(temp == '|') break;
                else score.add(temp);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        byte[] res = new byte[4];
        for(int i=0;i<score.size();i++)  res[i] = score.get(i);
        if(score.size()<4){
            res[score.size()] = '\t';
        }
        score.clear();
        return res;
    }
    public static String linkid(DataInputStream dis,LinkedList<Byte> id){
        while(true){
            try {
                byte temp = dis.readByte();
                if(temp == '|') break;
                else id.add(temp);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        byte[] res = new byte[id.size()];
        for(int i=0;i<id.size();i++)  res[i] = id.get(i);
        String stringid = new String(res);
        id.clear();
        return stringid;
    }
}

