package com.alibaba.middleware.race.sync;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by autulin on 6/17/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
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
