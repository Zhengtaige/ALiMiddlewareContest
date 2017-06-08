package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by autulin on 6/7/17.
 */
public class Test {
    private static final int mapLength = Integer.MAX_VALUE;

    public static void main(String[] args) throws IOException {
        MappedByteBuffer mappedByteBuffer = new RandomAccessFile(Constants.DATA_HOME + "/1.txt", "rw")
                .getChannel()
                .map(FileChannel.MapMode.READ_ONLY, 0, mapLength);
        mappedByteBuffer.load();

        int offset = 41;
        byte b;
        int i = 70; //由于每行数据一般至少会有一定长度的
//        do {
//            b = mappedByteBuffer.get(offset + i);
//            i++;
//        } while (b != '\n');
        boolean isFirst = true;
//        while (true) {
        mappedByteBuffer.position(offset);
        int t = 0;
        do {
            b = mappedByteBuffer.get(offset + t);
            t++;
        } while (b != '|');
        byte[] schemaBytes = new byte[t - 1];
        mappedByteBuffer.get(schemaBytes);
        System.out.println(new String(schemaBytes));

        offset = mappedByteBuffer.position() + 1;
        t = 0;
        do {
            b = mappedByteBuffer.get(offset + t);
            t++;
        } while (b != '|');
        byte[] tableBytes = new byte[t - 1];
        mappedByteBuffer.position(offset);
        mappedByteBuffer.get(tableBytes);
        System.out.println(new String(tableBytes));

        offset = mappedByteBuffer.position() + 1;
        mappedByteBuffer.position(offset);
        b = mappedByteBuffer.get();
        switch (b) {
            case 'I':
                System.out.println("insert");
                break;
            case 'U':
                System.out.println("update");
                break;
            case 'D':
                System.out.println("delete");
        }

//        }


    }
}
