package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Created by autulin on 6/9/17.
 */
public class FileReaderV2 {
    private static Logger logger = LoggerFactory.getLogger(FileReaderV2.class);
    private static int HEAD_LENGTH = 41; //用于读取时跳过
    private static int MIN_LENGTH = 75;

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
        mappedByteBuffer.position(600); //跳过文件结尾的"\0"

        System.out.println(readLine(mappedByteBuffer));
        System.out.println(readLine(mappedByteBuffer));
        System.out.println(readLine(mappedByteBuffer));
        System.out.println(readLine(mappedByteBuffer));
        System.out.println(readLine(mappedByteBuffer));
        System.out.println(readLine(mappedByteBuffer));

    }

    private static Row readLine(MappedByteBuffer buffer) {
        if (buffer.position() == 0) {
            return null;
        }
        int end = getLastEnterPostion(buffer);
        buffer.position(end - MIN_LENGTH); // 可以跳过一段
        int start = getLastEnterPostion(buffer);

//        byte[] bytes;
        Row row;
        if (start == 0) {
//            bytes = new byte[end - HEAD_LENGTH + 1]; //跳过|mysql-bin.000018470858532|1496828214000|
            buffer.position(start + HEAD_LENGTH - 1);
//            buffer.get(bytes);
            row = parseRow(buffer, "middleware5", "student", 0);
            buffer.position(start);
        } else {
//            bytes = new byte[end - start - HEAD_LENGTH];
            buffer.position(start + HEAD_LENGTH);
//            buffer.get(bytes);
            row = parseRow(buffer, "middleware5", "student", 0);
            buffer.position(start + 1);
        }

        return row;
    }

    private static Row parseRow(MappedByteBuffer buffer, String schema, String table, int end) {
        Row row = new Row();
        byte[] tSchema = FileReader.readArea(buffer, 1);
        if (!Arrays.equals(tSchema, schema.getBytes())) {
            return null;
        }
        byte[] tTable = FileReader.readArea(buffer, 1);
        if (!Arrays.equals(tTable, table.getBytes())) {
            return null;
        }

        buffer.get();
        byte operation = buffer.get();
        row.setOperation((char) operation);

        readColunms(buffer, row, 0);
        return row;
    }

    private static void readColunms(MappedByteBuffer buffer, Row row, int end) {
        int position = buffer.position();

        byte[] bytes;

//        System.out.println(buffer.get());
        //id
        position = getNextSeperatorPosition(buffer, 2) + 1;
        buffer.position(position);
        int areaEnd = getNextSeperatorPosition(buffer, 1);
        Column id = new Column();
        id.setName("id");
        id.setPrimary(true);
        id.setType(Column.INT_TYPE);
        if (buffer.get(position) != 'N') {
            bytes = new byte[areaEnd - position];
            buffer.get(bytes);
            id.setBefore(bytes);
        } else {
            position = getNextSeperatorPosition(buffer, 1) + 1;
            buffer.position(position);
        }
        areaEnd = getNextSeperatorPosition(buffer, 1);
        if (buffer.get(position) != 'N') {
            bytes = new byte[areaEnd - position];
            buffer.get(bytes);
            id.setAfter(bytes);
        } else {
            position = getNextSeperatorPosition(buffer, 1) + 1;
            buffer.position(position);
        }
        row.getColumns().add(id);

        switch (row.getOperation()) {
            case 'I':
                //first_name
                position = getNextSeperatorPosition(buffer, 3) + 1;
                buffer.position(position);
                areaEnd = getNextSeperatorPosition(buffer, 1);
                Column firstName = new Column();
                firstName.setName("first_name");
                firstName.setPrimary(false);
                firstName.setType(Column.STRING_TYPE);
                bytes = new byte[areaEnd - position];
                buffer.get(bytes);
                firstName.setAfter(bytes);
                row.getColumns().add(firstName);

                //last_name
                position = getNextSeperatorPosition(buffer, 3) + 1;
                buffer.position(position);
                areaEnd = getNextSeperatorPosition(buffer, 1);
                Column lastName = new Column();
                lastName.setName("last_name");
                lastName.setPrimary(false);
                lastName.setType(Column.STRING_TYPE);
                bytes = new byte[areaEnd - position];
                buffer.get(bytes);
                lastName.setAfter(bytes);
                row.getColumns().add(lastName);

                //sex
                position = getNextSeperatorPosition(buffer, 3) + 1;
                buffer.position(position);
                areaEnd = getNextSeperatorPosition(buffer, 1);
                Column sex = new Column();
                sex.setName("sex");
                sex.setPrimary(false);
                sex.setType(Column.STRING_TYPE);
                bytes = new byte[areaEnd - position];
                buffer.get(bytes);
                sex.setAfter(bytes);
                row.getColumns().add(sex);

                //score
                position = getNextSeperatorPosition(buffer, 3) + 1;
                buffer.position(position);
                areaEnd = getNextSeperatorPosition(buffer, 1);
                Column score = new Column();
                score.setName("score");
                score.setPrimary(false);
                score.setType(Column.INT_TYPE);
                bytes = new byte[areaEnd - position];
                buffer.get(bytes);
                score.setAfter(bytes);
                row.getColumns().add(score);
                break;

            case 'U':
                Column column;
                while ((column = readNextColumn(buffer)) != null) {
                    row.getColumns().add(column);
                }
                break;
        }

    }

    private static Column readNextColumn(MappedByteBuffer buffer) {
        Column column = new Column();
        buffer.get();
        column.setPrimary(false);
        switch (buffer.get()) {
            case 'f': //first_name
                column.setName("first_name");
                column.setType(Column.STRING_TYPE);
                break;
            case 'l':
                column.setName("last_name");
                column.setType(Column.STRING_TYPE);
                break;
            case 's':
                if (buffer.get() == 'e') { //sex
                    column.setName("sex");
                    column.setType(Column.STRING_TYPE);
                } else {
                    column.setName("score");
                    column.setType(Column.INT_TYPE);
                }
                break;
            case '\n':
                return null;
        }
        int start = getNextSeperatorPosition(buffer, 1) + 1;
        int end = getNextSeperatorPosition(buffer, 2);
        byte[] bytes;
        if (buffer.get(start) == 'N') {
            column.setBefore(null);
        } else {
            bytes = new byte[end - start];
            buffer.position(start);
            buffer.get(bytes);
            column.setBefore(bytes);
        }
        start = getNextSeperatorPosition(buffer, 1) + 1;
        end = getNextSeperatorPosition(buffer, 2);
        if (buffer.get(start) == 'N') {
            column.setAfter(null);
        } else {
            bytes = new byte[end - start];
            buffer.position(start);
            buffer.get(bytes);
            column.setAfter(bytes);
        }
        return column;
    }

//    private static void godV(MappedByteBuffer buffer, String schema, String table) {
//        int end = buffer.position();
//        int t = getLastSeperatorPosition(buffer);
//        List<Column> columns = new LinkedList<>();
//        while (true) {
//            if (buffer.get(end - 1) == 'L') {
//
//            }
//        }
//    }


    /**
     * 反向查找上一个换行副
     *
     * @param buffer b
     * @return 换行副位置
     */
    private static int getLastEnterPostion(MappedByteBuffer buffer) {
        int position = buffer.position();
        while (position > 0 && buffer.get(--position) != '\n') {
        }
        return position;
    }


    private static int getNextSeperatorPosition(MappedByteBuffer buffer, int num) {
        int position = buffer.position();
        int i = 0;
        while (true) {
            if (buffer.get(position++) == '|') {
                if (++i == num) {
                    return position - 1;
                }
            }
        }
    }
}
