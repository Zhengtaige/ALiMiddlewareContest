package com.alibaba.middleware.race.sync;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by autulin on 6/17/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
//        RandomAccessFile randomAccessFile = new RandomAccessFile("/home/admin/middle/7721890v3f/Result.rs", "r");
//        randomAccessFile.skipBytes((int) (randomAccessFile.length() - 3));
//        System.out.println(randomAccessFile.read());
//        System.out.println(randomAccessFile.read());
//        System.out.println(randomAccessFile.read());
//        System.out.println(randomAccessFile.read());
        String name = "彭, 周, 江, 柳, 林, 陈, 高, 阮, 侯, 赵, 李, 王, 徐, 郑, 杨, 刘, 邹, 吴, 钱, 孙";
        String lastname = "铭, 闵, 静, 天, 甲, 民, 田, 力, 立, 黎, 城, 刚, 骏, 四, 九, 甜, 雨, 他, 明, 乙, 军, 恬, 俊, 乐, 娥, 励, 莉, 兲, 景, 人, 益, 发, 十, 敏, 八, 晶, 六, 京, 名, 丙, 五, 三, 上, 我, 成, 君, 诚, 一, 丁, 七, 二, 依";


        HashSet<String> nameSet = new HashSet<>();
        HashSet<Byte> nameLastByteSet = new HashSet<>();

        for (String s :
                lastname.split(", ")) {
            nameSet.add(s);
//            System.out.print(s + ":");
            nameLastByteSet.add(s.getBytes()[2]);
//            for (byte b :
//                    s.getBytes()) {
//                System.out.print(b);
//                System.out.print(",");
//            }
//            System.out.println();
        }
        System.out.println("name size: " + nameSet.size());
        System.out.println("lastByte size: " + nameLastByteSet.size());

        System.out.println("-------------------");
        for (String s :
                lastname.split(", ")) {
            System.out.print(s + ":");
            for (byte b :
                    s.getBytes()) {
                System.out.print(b);
                System.out.print(",");
            }
            System.out.println();
        }
//        System.out.println(name.split(", ")[0].getBytes());
    }

    public static void main11(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        int headLength = "|mysql-bin.000018470858532|1496828214000|middleware5|student|".length();
        FileInputStream fileInputStream = new FileInputStream(new File(Constants.TESTER_HOME + "/1.txt"));
        BufferedInputStream inputStream = new BufferedInputStream(fileInputStream, 1 * 1024 * 1024);
        FileOutputStream fileOutputStream = new FileOutputStream(new File(Constants.TESTER_HOME + "/1_copy.txt"));
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream, 1 * 1024 * 1024);
        while (inputStream.available() > 0) {
//            inputStream.skip(headLength);
//            int b = inputStream.read();
//            if (b == 'I' || b == 'U' || b == 'D') {
//                do {
//                    outputStream.write(b);
//                    b = inputStream.read();
//                }while (b != '\n');
//                outputStream.write(b);
//            }
            outputStream.write(inputStream.read());
        }
        outputStream.close();
        fileInputStream.close();
        inputStream.close();
        fileInputStream.close();
        long end = System.currentTimeMillis();
        System.out.printf("cost: %d", end - start);
    }

    public static String getRandomFileName() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("mmssHHSSS");
        Date date = new Date();
        String s = simpleDateFormat.format(date);
        int random = (int) (new Random().nextDouble() * (99999 - 10000 + 1)) + 10000;
        return random + s;
    }

}
