package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by autulin on 6/9/17.
 */
public class FileReaderV2 {
    private static Logger logger = LoggerFactory.getLogger(FileReaderV2.class);

    public static void main(String[] args) {
        doRead(new File(Constants.DATA_HOME + "/" + "1.txt"), "middleware5", "student", 100, 200);
    }

    public static void doRead(File file, String schema, String table, int start, int end) {
        Long startTime = System.currentTimeMillis();
        MappedByteBuffer mappedByteBuffer = null;
        try {
            mappedByteBuffer = new RandomAccessFile(file, "r")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        } catch (IOException e) {
            logger.error(e.toString());
        }
        mappedByteBuffer.load();

        int offset = (int) file.length();
        mappedByteBuffer.position(300);

        System.out.println(readLine(mappedByteBuffer));
        System.out.println(readLine(mappedByteBuffer));

    }

    private static String readLine(MappedByteBuffer buffer) {
        byte b;
        int i = 0;
        int offset = buffer.position();
        do {
            b = buffer.get(--offset);
            i++;
        } while (b != '\n' && offset > 0);
        byte[] bytes = new byte[i - 1];
        buffer.position(offset + 1);
        buffer.get(bytes);
//        System.out.println(FileReader.readStringArea(buffer, 3));
//        System.out.println(FileReader.readStringArea(buffer, 1));
//        System.out.println(FileReader.readArea(buffer, 1)[0]);
//        System.out.println(FileReader.readColume(buffer));
//        System.out.println(FileReader.readIntArea(buffer, 2));
//        System.out.println(FileReader.readColume(buffer));
//        System.out.println(FileReader.readStringArea(buffer, 2));
//        System.out.println(FileReader.readColume(buffer));
//        System.out.println(FileReader.readStringArea(buffer, 2));
//        System.out.println(FileReader.readColume(buffer));
//        System.out.println(FileReader.readStringArea(buffer, 2));
//        System.out.println(FileReader.readColume(buffer));
//        System.out.println(FileReader.readIntArea(buffer, 2));

        buffer.position(offset);
        return new String(bytes);
    }

    private static void sao(MappedByteBuffer buffer) {
        int position = buffer.position();
        while (true) {
            if (buffer.get(position) == '|') {

            } else {
                position--;
            }
        }
    }
}
