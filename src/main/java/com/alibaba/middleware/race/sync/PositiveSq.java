package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.ResultMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

/**
 * Created by nick_zhengtaige on 2017/6/16.
 */
public class PositiveSq {
    static Logger logger = LoggerFactory.getLogger(PositiveSq.class);
    private static int Length = 55;
    private static HashMap<Byte, Byte> typemap = new HashMap<>();   //记录操作类型以及第几列属性
    private static long beforeid;
    private static long afterid ;
    private static byte operation ;
    private static byte[] male = {-25, -108, -73};
    private static byte[] female = {-27, -91, -77};
    private static byte type;
    public static boolean resultReleased = false;
    private static ResultMap resultMap=new ResultMap(Server.startPkId,Server.endPkId); ;
    public static void main(String[] args) throws IOException {
        testTime();
   }
    public static void testTime(){
        long t1 = System.currentTimeMillis();
        initMap();
        try {
            positiveread();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("{}", System.currentTimeMillis()-t1);
    }

    public static void positiveread() throws IOException {
        for (int i = 1; i <= 10; i++) {
            try {
                FileChannel fileChannel = new RandomAccessFile(Constants.DATA_HOME+(i-1), "r").getChannel();
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                while (true) {
                    //Step1: 读取废字段
                    mappedByteBuffer.position(mappedByteBuffer.position() + Length);
                    handleIUD(mappedByteBuffer);
                }
            } catch (IllegalArgumentException e){
                logger.info("{}",i+"文件读取完毕!");
            }
            catch (BufferUnderflowException e){
                logger.info("{}",i+"文件读取完毕!");
            }
        }
        releaseResult();
    }

    private static void handleIUD(MappedByteBuffer mappedByteBuffer) throws IOException {
        while (true) {
        operation=mappedByteBuffer.get();
        //Step2: 读取操作符
        switch (operation) {
            case 'I':
                mappedByteBuffer.position(mappedByteBuffer.position()+13);
                afterid = linkid(mappedByteBuffer);        //读 id

                if(!Utils.isInRange(afterid)){
                    mappedByteBuffer.position(mappedByteBuffer.position()+99);
                    while(mappedByteBuffer.get()!='\n');
                    return;
                }

                mappedByteBuffer.position(mappedByteBuffer.position()+20);

                byte [][]result = resultMap.get(afterid);
                result[0] = new byte[3];
                mappedByteBuffer.get(result[0]);      //读 姓

                mappedByteBuffer.position(mappedByteBuffer.position()+20);
                result[1] = new byte[6];
                linkname(mappedByteBuffer,result[1]);

                mappedByteBuffer.position(mappedByteBuffer.position()+13);
                byte sex = mappedByteBuffer.get();
                result[2] = new byte[3];
                if (sex == -25) {
                    result[2] = male;
                } else {
                    result[2] = female;
                }

                mappedByteBuffer.position(mappedByteBuffer.position() + 2);

                mappedByteBuffer.position(mappedByteBuffer.position()+16);
                result[3] = new byte[6];
                linkscore(mappedByteBuffer,result[3]);

                mappedByteBuffer.position(mappedByteBuffer.position()+16);
                //ztg
                result[4] = new byte[6];
                linkscore(mappedByteBuffer,result[4]);
                mappedByteBuffer.get();
                return;

            case 'U':
                mappedByteBuffer.position(mappedByteBuffer.position()+8);
                beforeid = linkid(mappedByteBuffer);
                afterid = linkid(mappedByteBuffer);
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
                        resultMap.remove(beforeid);
                    return;
                }
                byte [][]olddata = resultMap.get(beforeid);
                while (mappedByteBuffer.get() != '\n') {
                    type = typemap.get(mappedByteBuffer.get());  //读到类型
                    if (type == 0) {
                        mappedByteBuffer.position(mappedByteBuffer.position()+17);
                        mappedByteBuffer.get(olddata[0],0,3);
                        mappedByteBuffer.position(mappedByteBuffer.position()+1);
                    } else if (type == 1) {
                        mappedByteBuffer.position(mappedByteBuffer.position()+15);
                        while(mappedByteBuffer.get()!='|');
                        linkname(mappedByteBuffer,olddata[1]);
                    } else if (type == 2) {
                        mappedByteBuffer.position(mappedByteBuffer.position()+10);
                        sex = mappedByteBuffer.get();
                        if (sex == -25) {
                            olddata[2]=male;
                        } else {
                            olddata[2]=female;
                        }
                        mappedByteBuffer.position(mappedByteBuffer.position() + 3);
                    } else {
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
                        linkscore(mappedByteBuffer,olddata[type]);
                    }
                }
                if(beforeid!=afterid){
                    resultMap.remove(beforeid);
                    resultMap.putArray(afterid,olddata);
                }
                return;

            case 'D':
                mappedByteBuffer.position(mappedByteBuffer.position()+8);
                beforeid = linkid(mappedByteBuffer);

                if(!Utils.isInRange(beforeid)){
                    mappedByteBuffer.position(mappedByteBuffer.position()+104);
                    while(mappedByteBuffer.get()!='\n');
                    return;
                }
                mappedByteBuffer.position(mappedByteBuffer.position()+104);
                while (mappedByteBuffer.get() != '\n') ;
                resultMap.remove(beforeid);
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

    public static void linkname(MappedByteBuffer mappedByteBuffer, byte []result) {
        mappedByteBuffer.get(result, 0, 3);
        byte temp = mappedByteBuffer.get();
        if (temp != '|') {
            result[3] = temp;
            mappedByteBuffer.get(result, 4, 2);
            mappedByteBuffer.get();
        }else{
            result[3] = '\t';
        }
    }

    public static void linkscore(MappedByteBuffer mappedByteBuffer, byte []result) {
        int tol = 0;
        while(true){
            byte temp = mappedByteBuffer.get();
            if(temp == '|') break;
            else {
                result[tol] = temp;
                tol++;
            }
        }
        if(tol==6){
            return ;
        }
        result[tol] = '\t';
        return ;
    }

    public static long linkid(MappedByteBuffer mappedByteBuffer) {    //ztg尝试修改
        long bitch = 0;
        byte temp = mappedByteBuffer.get();
        if (temp == '8' || temp == '9') {
            while (mappedByteBuffer.get() != '|') ;
            return bitch;
        } else {
            bitch = bitch * 10 + (temp - 48);
        }
        while (true) {
            temp = mappedByteBuffer.get();
            if (temp == '|') break;
            else {
                bitch = bitch * 10 + (temp - 48);
            }
        }
        return bitch;
    }

    public static void releaseResult() {
        logger.info("[{}]start release result", System.currentTimeMillis());
        FileChannel channel;
        try {
            int num = 0 ;
            RandomAccessFile randomAccessFile = new RandomAccessFile(new File(Constants.MIDDLE_HOME + Constants.RESULT_FILE_NAME), "rw");
            channel = randomAccessFile.getChannel();
            for (long i = Server.startPkId + 1; i < Server.endPkId ; i++) {
                byte[][] colomns = resultMap.get(i);
                if (colomns[0]!=null) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(256);
                    byteBuffer.put(String.valueOf(i).getBytes()); // id
                    byteBuffer.put((byte) 9); // \t
                    for (int j = 0; j < colomns.length - 1; j++) {
                        if(j == 1 || j==3 || j==4){
                            for (byte tmp:
                                    colomns[j]) {
                                if(tmp == '\t'){
                                    break;
                                }else{
                                    byteBuffer.put(tmp);
                                }
                            }
                        }else{
                            try {
                                byteBuffer.put(colomns[j]);
                            }catch (Exception e){
                                System.out.println();
                            }

                        }
                        byteBuffer.put((byte) 9); // \t
                    }
                    byteBuffer.put(colomns[colomns.length - 1]);
                    byteBuffer.put((byte) 10);
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
}


