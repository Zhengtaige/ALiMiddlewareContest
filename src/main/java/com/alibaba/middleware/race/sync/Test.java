package com.alibaba.middleware.race.sync;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by autulin on 6/17/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        read();
//        RandomAccessFile randomAccessFile = new RandomAccessFile("/home/admin/middle/7721890v3f/Result.rs", "r");
//        randomAccessFile.skipBytes((int) (randomAccessFile.length() - 3));
//        System.out.println(randomAccessFile.read());
//        System.out.println(randomAccessFile.read());
//        System.out.println(randomAccessFile.read());
//        System.out.println(randomAccessFile.read());
//        List<String> test = new LinkedList<>();
//        test.add("1");
//        test.add("2");
//        test.add("3"s);
//        test.add("1");
//        System.out.println(test.toString());
//        int i = 0;
//        int[] nums = new int[1000];
//        int[] test = new int[1000];
//        for (int j = 0; j < test.length; j++) {
//            test[j] = new Random().nextInt();
//        }
//
//        ExecutorService service = Executors.newSingleThreadExecutor();
//        service.submit(new Runnable() {
//            @Override
//            public void run() {
//
//            }
//        });
//        CompletionService cs = new ExecutorCompletionService(service);
//        cs.submit(new Callable() {
//            @Override
//            public Object call() throws Exception {
//                i++;
//                return null;
//            }
//        })

    }

    public static void read() throws IOException {
        long start = System.currentTimeMillis();
        for (int i = 1; i < 11; i++) {
            FileChannel fileChannel = new RandomAccessFile(Constants.DATA_HOME + "/" + i + ".txt", "r").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());


            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            try {
                while (true) {
                    byte b = mappedByteBuffer.get();
                    if (b != '\n') byteBuffer.put(b);
                    else {
                        byteBuffer.clear();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("cost:" + (end - start));
    }

    public static byte[] readLine(MappedByteBuffer buffer) throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        byte b;
        while ((b = buffer.get()) != '\n') {
            byteBuffer.put(b);
        }
        return byteBuffer.array();
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
