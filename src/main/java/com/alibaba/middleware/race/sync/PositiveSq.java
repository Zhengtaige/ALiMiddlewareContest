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

    private static byte[] readdata = new byte[13];
    private static byte[] readupdate = new byte[17];
    private static HashMap<Byte, Byte> typemap = new HashMap<Byte, Byte>();   //记录操作类型以及第几列属性
    private static int Length = "|mysql-bin.000018829057528|1497264609000|middleware5|student|".getBytes().length;
    private static long reflectarea = 0;
    private static LinkedList<Byte> namelist = new LinkedList<Byte>();
    private static String beforeid=null;
    private static String afterid = null;
    public static void main(String[] args) throws IOException {
        long t1 = System.currentTimeMillis();
        initMap();
        positiveread(new File("/Users/Nick_Zhengtaige/Desktop/A/canal.txt"));
        System.out.println(System.currentTimeMillis()-t1);
    }

    public static void positiveread(File file) {
        Long startTime = System.currentTimeMillis();
        byte[] wastewords = new byte[Length];
        byte[] operation = new byte[1];
        byte[] first = new byte[3];
        byte[] last = new byte[6];
        byte[] sex = new byte[1];
        byte[] score = new byte[3];
        byte[] type = new byte[1];
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file), Utils.Buffer_Size));
            while(true){
                    //Step1: 读取废字段
                     int stop = inputStream.read(wastewords);
                     if(stop == -1) return;
                   //Step2: 读取操作符
                    inputStream.read(operation);
                    switch (operation[0]){
                        case 'I':
                            passline(inputStream,3);
                            beforeid = linkid(inputStream,namelist);        //读 id
                            passline(inputStream,2);
                            inputStream.read(first);      //读 姓
                            passline(inputStream,3);
                            System.arraycopy(linkname(inputStream,namelist),0,last,0,6);   //读 名
                            passline(inputStream,2);
                            if(inputStream.readChar() == '男') sex[0] = 0;   //读性别 男 0 女 1
                            else sex[0] = 1;
                            passline(inputStream,3);
                            System.arraycopy(linkscore(inputStream,namelist),0,score,0,3);   //读 分 数
                            System.arraycopy(first,0,readdata,0,3);
                            System.arraycopy(last,0,readdata,3,6);
                            System.arraycopy(sex,0,readdata,9,1);
                            System.arraycopy(score,0,readdata,10,3);
                            inputStream.readByte(); //吃掉 '\n'
                            break;

                        case 'U':
                            passline(inputStream,2);
                            beforeid = linkid(inputStream,namelist);
                            afterid = linkid(inputStream,namelist);
                            int pos = 0;
                            while(inputStream.readByte()!='\n')
                            {
                                type[0] = typemap.get(inputStream.readByte());  //读到类型
                                System.arraycopy(type,0,readupdate,pos,1);
                                pos++;
                                passline(inputStream,2);
                                if(type[0] == 1) {
                                    inputStream.read(first);
                                    System.arraycopy(first,0,readupdate,pos,3);
                                    pos+=3;
                                    inputStream.readByte();
                                }
                                else if(type[0] ==2){
                                    System.arraycopy(linkname(inputStream,namelist),0,readupdate,pos,6);   //读 名
                                    pos+=6;
                                }
                                else if(type[0] ==3){
                                    if(inputStream.readChar() == '男') sex[0] = 0;   //读性别 男 0 女 1
                                    else sex[0] = 1;
                                    System.arraycopy(sex,0,readupdate,pos,1);
                                    pos++;
                                    inputStream.readByte();
                                }
                                else{
                                    System.arraycopy(linkscore(inputStream,namelist),0,readupdate,pos,3);   //读 分 数
                                    pos+=3;
                                }
                            }
                            if(pos<16) readupdate[pos] = '\t';
                            break;

                        case 'D' :
                            passline(inputStream,2);
                            beforeid = linkid(inputStream,namelist);
                            while(inputStream.readByte()!='\n');

                    }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        }
    public static void initMap() {
        typemap.put((byte)'i', (byte)1);
        typemap.put((byte)'a', (byte)2);
        typemap.put((byte)'e', (byte)3);
        typemap.put((byte)'c', (byte)4);

//        typelength.put(1,3);
//        typelength.put(2,6);
//        typelength.put(3,1);
//        typelength.put(4,2);

    }
    public static void passline(DataInputStream dis, int num){
        while(true){
            try {
                if(dis.readByte()=='|'){
                    num--;
                }
                if(num == 0) return;
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        byte[] res = new byte[3];
        if(score.size()>3){
            System.out.println(score.size());
        }
        else {
            for(int i=0;i<score.size();i++)  res[i] = score.get(i);
        }
        if(score.size()<3){
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

