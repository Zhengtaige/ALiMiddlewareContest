package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

/**
 * Created by autulin on 6/9/17.
 */
public class GodVReader {
    //    static boolean i = false;
//    static boolean u = false;
//    static boolean d = false;
    private static Logger logger = LoggerFactory.getLogger(GodVReader.class);
    private static int HEAD_LENGTH = 60; //用于读取时跳过
    private static int MIN_LENGTH = 75;
    private static GodVReader INSTANCE = new GodVReader();
    public boolean done = false;
    private int start;
    private int end;
    private String[] tableStructure = {"id", "first_name", "last_name", "sex", "score"};
    private HashMap<Long, HashMap<String, byte[]>> finalMap = new HashMap<>();
    private HashMap<Long, Long> primaryKeyMap = new HashMap<>(); //key为要追溯的，value为最终值

    private boolean init = false;

    public static GodVReader getINSTANCE(int start, int end) {

        INSTANCE.start = start;
        INSTANCE.end = end;

        INSTANCE.init = true;
        return INSTANCE;
    }

    public static GodVReader getINSTANCE() {
//        if (!INSTANCE.init) return null;
        return INSTANCE;
    }

    public static void main(String[] args) {
        getINSTANCE(100, 1000000).doRead("1.txt");
        getINSTANCE().getResult();
    }

    public void doRead(String sourceFileName) {
        Long startTime = System.currentTimeMillis();
        MappedByteBuffer mappedByteBuffer = null;
        Path middleFilePath = Paths.get(Constants.MIDDLE_HOME + "/" + sourceFileName);
        File middleFile = new File(middleFilePath.toString());
        try {
            Files.copy(Paths.get(Constants.DATA_HOME + "/" + sourceFileName),
                    middleFilePath);
            logger.info("copy file[{}] cost: {}", sourceFileName, (System.currentTimeMillis() - startTime));
            mappedByteBuffer = new RandomAccessFile(middleFile, "r")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, 0, middleFile.length());
        } catch (IOException e) {
            logger.error(e.toString());
            return;
        }
        mappedByteBuffer.load();

        int offset = (int) middleFile.length();
        mappedByteBuffer.position(offset); //跳过文件结尾的"\0"


        for (long i = start + 1; i < end; i++) {
            primaryKeyMap.put(i, i);
        }

        Row row;
        while (primaryKeyMap.size() != 0) {
            row = readLine(mappedByteBuffer);
            if (row == null) {
                logger.error("文件读完了！");
                break;
            }
//            if (row.isValid()) {
                doJob(row);
//            }
        }

        Long endTime = System.currentTimeMillis();
        logger.info("file[{}] done cost: {}", sourceFileName, (endTime - startTime));

//        getResult();
    }

    public byte[] getResultBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(7 * 1024 * 1024); //考虑到100w条
        for (long i = start + 1; i < end; i++) {
            if (finalMap.containsKey(i)) {
                HashMap<String, byte[]> colomns = finalMap.get(i);
                for (int j = 0; j < tableStructure.length - 1; j++) {
                    byteBuffer.put(colomns.get(tableStructure[j]));
                    byteBuffer.put((byte) 9); // \t
                }
                byteBuffer.put(colomns.get(tableStructure[tableStructure.length - 1]));
                byteBuffer.put((byte) 10); // \n

            }
        }
        byteBuffer.flip();
        byte[] bytes = new byte[byteBuffer.limit()];
        byteBuffer.get(bytes);
        return bytes;
    }

    public void getResult() {
        FileChannel channel;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(new File(Constants.MIDDLE_HOME + Constants.RESULT_FILE_NAME), "rw");
            channel = randomAccessFile.getChannel();
            for (long i = start + 1; i < end; i++) {
                if (finalMap.containsKey(i)) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(256);
                    HashMap<String, byte[]> colomns = finalMap.get(i);
                    for (int j = 0; j < tableStructure.length - 1; j++) {
                        byteBuffer.put(colomns.get(tableStructure[j]));
                        byteBuffer.put((byte) 9); // \t
                    }
                    byteBuffer.put(colomns.get(tableStructure[tableStructure.length - 1]));
                    byteBuffer.put((byte) 10); // \n
                    byteBuffer.flip();
//                    System.out.println(byteBuffer.position() + "," + byteBuffer.limit());
//                    System.out.println(new String(byteBuffer.array()));
                    while (byteBuffer.hasRemaining()) {
                        channel.write(byteBuffer);
                    }
                }
            }
            channel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void doJob(Row row) {
        long idBefore, idAfter;
        List<Column> columns = row.getColumns();
        switch (row.getOperation()) {
            case 'I':
                idAfter = Utils.bytes2Long(columns.get(0).getAfter());
                if (primaryKeyMap.containsKey(idAfter)) {
                    HashMap<String, byte[]> columnMap;
                    long finalId = primaryKeyMap.get(idAfter);
                    if (!finalMap.containsKey(finalId)) { //直接插入
                        columnMap = new HashMap<>();
                        for (Column c :
                                columns) {
                            columnMap.put(c.getName(), c.getAfter());
                        }
                        finalMap.put(finalId, columnMap);
                    } else { //追溯到的插入
                        columnMap = finalMap.get(finalId);
                        for (Column c :
                                columns) {
                            if (!columnMap.containsKey(c.getName())) { //只插入不包含的列，既然是insert，那么这次肯定会插满的
                                columnMap.put(c.getName(), c.getAfter());
                            }
                        }
                    }
                    primaryKeyMap.remove(idAfter); //停止追溯该id
                }
                break;
            case 'D':
                idBefore = Utils.bytes2Long(columns.get(0).getBefore());
                if (primaryKeyMap.containsKey(idBefore)) { //停止追溯该id
                    primaryKeyMap.remove(idBefore);
                }
                break;
            case 'U':
                idAfter = Utils.bytes2Long(columns.get(0).getAfter());
                idBefore = Utils.bytes2Long(columns.get(0).getBefore());

                if (primaryKeyMap.containsKey(idBefore)
                        && !primaryKeyMap.containsKey(idAfter)) { // 从范围里面改出去的，先删掉里面的追溯
                    primaryKeyMap.remove(idBefore);
                    break;
                }
                if (primaryKeyMap.containsKey(idAfter)) { //得到追溯的ID
                    long id = primaryKeyMap.get(idAfter); //原始ID
                    HashMap<String, byte[]> columnMap = finalMap.get(id);
                    if (columnMap == null) { //从外面改进来, 或者里面改里面
                        columnMap = new HashMap<>();
                        finalMap.put(id, columnMap);
                    }
                    for (Column c :
                            columns) {
                        if (!columnMap.containsKey(c.getName())) {
                            columnMap.put(c.getName(), c.getAfter());
                        }
                    }


                    primaryKeyMap.remove(idAfter); //默认处理后删掉当前id，如果列没有更新完就向上追溯
                    if (columnMap.size() != 5) {
                        primaryKeyMap.put(idBefore, id);  //向上追溯
                    }

                }


        }

    }

    private Row readLine(MappedByteBuffer buffer) {
        if (buffer.position() == 0) {
            return null;
        }
        int end = getLastEnterPostion(buffer);
        buffer.position(end - MIN_LENGTH); // 可以跳过一段
        int start = getLastEnterPostion(buffer);

        Row row;
        if (start == 0) {
            buffer.position(start + HEAD_LENGTH - 1);
            row = parseRow(buffer);
            buffer.position(start);
        } else {
            buffer.position(start + HEAD_LENGTH);
            row = parseRow(buffer);
            buffer.position(start + 1);
        }

        //调试日志
//        if (!i || !u || !d) {
//            switch (row.getOperation()) {
//                case 'I':
//                    if (!i) {
//                        logger.info(row.toString());
//                    }
//                    i = true;
//                    break;
//                case 'U':
//                    if (!u) {
//                        logger.info(row.toString());
//                    }
//                    u = true;
//                    break;
//                case 'D':
//                    if (!d) {
//                        logger.info(row.toString());
//                    }
//                    d = true;
//                    break;
//            }
//        }

        return row;
    }

    private Row parseRow(MappedByteBuffer buffer) {
        Row row = new Row();
        //表已经定死，不需要了
//        byte[] tSchema = FileReader.readArea(buffer, 1);
//        if (!Arrays.equals(tSchema, schema.getBytes())) {
//            row.setValid(false);
//            return row;
//        }
//        byte[] tTable = FileReader.readArea(buffer, 1);
//        if (!Arrays.equals(tTable, table.getBytes())) {
//            row.setValid(false);
//            return row;
//        }

//        row.setValid(true);

        buffer.position(getNextSeperatorPosition(buffer, 1) + 1);
        byte operation = buffer.get();
        row.setOperation((char) operation);

        readColumns(buffer, row);
        return row;
    }

    private void readColumns(MappedByteBuffer buffer, Row row) {
        int position;
        byte[] bytes;

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
            buffer.get(); //偏移一位
            position = buffer.position();
        } else {
            position = getNextSeperatorPosition(buffer, 1) + 1;
            buffer.position(position);
        }

        if (buffer.get(position) != 'N') {
            areaEnd = getNextSeperatorPosition(buffer, 1);
            bytes = new byte[areaEnd - position];
            buffer.get(bytes);
            id.setAfter(bytes);
        } else {
            position = getNextSeperatorPosition(buffer, 1);
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

    private Column readNextColumn(MappedByteBuffer buffer) {
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
        buffer.position(end);
        start = getNextSeperatorPosition(buffer, 1) + 1;
        if (buffer.get(start) == 'N') {
            column.setAfter(null);
        } else {
            end = getNextSeperatorPosition(buffer, 2);
            bytes = new byte[end - start];
            buffer.position(start);
            buffer.get(bytes);
            column.setAfter(bytes);
        }
        return column;
    }



    /**
     * 反向查找上一个换行副
     *
     * @param buffer b
     * @return 换行副位置
     */
    private int getLastEnterPostion(MappedByteBuffer buffer) {
        int position = buffer.position();
        while (position > 0 && buffer.get(--position) != '\n') {
        }
        return position;
    }


    private int getNextSeperatorPosition(MappedByteBuffer buffer, int num) {
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

    public HashMap<Long, HashMap<String, byte[]>> getFinalMap() {
        return finalMap;
    }
}
