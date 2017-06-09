package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by autulin on 6/7/17.
 */
public class FileReader {
    //    private static final int mapLength = Integer.MAX_VALUE;
    private static Logger logger = LoggerFactory.getLogger(FileReader.class);

    public static void main(String[] args) throws IOException {
        readOneFile(new File(Constants.DATA_HOME + "/" + "1.txt"), "middleware5", "student", 100, 200);
    }

    public static void readOneFile(File file, String schema, String table, int start, int end) {
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

        boolean gotI = false;
        boolean gotU = false;
        boolean gotD = false;

        int totalOperationForTable = 0; //查看对于此表有多少个操作
        int totalOperationForStarAndEnd = 0;
        int totalOperationUpdatePK = 0;
        int totalOutToIn = 0;
        int totalInArrangeKeyChange = 0;
        Set<String> schemaSets = new HashSet<>();
        Set<String> tableSets = new HashSet<>();
        logger.info("start read {}", file.getName());
        for (; mappedByteBuffer.hasRemaining(); ) {
            if (mappedByteBuffer.get(mappedByteBuffer.position()) == '\0') { //尝试读取下一个byte（不改变position），”\0“为文件结束
                logger.info("read done!");
                break;
            }

            String tSchema = readStringArea(mappedByteBuffer, 3);
            if (!schemaSets.contains(tSchema)) {
                schemaSets.add(tSchema);
            }
            if (!schema.equals(tSchema)) {
                readLine(mappedByteBuffer, false);
                continue;
            }

            String tTable = readStringArea(mappedByteBuffer, 1);
            if (!tableSets.contains(tTable)) tableSets.add(tTable);
            if (!table.equals(tTable)) {
                readLine(mappedByteBuffer, false);
                continue;
            } else totalOperationForTable++;

            byte operation = readArea(mappedByteBuffer, 1)[0];

            //先把主键读了
            readColume(mappedByteBuffer);
            //修改主键的情况
            int keyBefore = readIntArea(mappedByteBuffer, 1);
            int keyAfter = readIntArea(mappedByteBuffer, 1);

            //是否在区间内操作的统计
            switch (operation) {
                case 'I':
                    if (keyAfter > start && keyAfter < end) {
                        totalOperationForStarAndEnd++;
                    }
                    break;
                case 'D':
                    if (keyBefore > start && keyBefore < end) {
                        totalOperationForStarAndEnd++;
                    }
                    break;
                case 'U':
                    if ((keyBefore > start && keyBefore < end) || (keyBefore > start && keyBefore < end)) {
                        totalOperationForStarAndEnd++;
                        if (keyBefore != keyAfter){ //改主键的行为
                            totalOperationUpdatePK++;
                            if ((keyBefore > end || keyBefore < start) && (keyAfter > start && keyAfter < end)) //外面跑里面
                                totalOutToIn++;
                            if ((keyBefore < end && keyBefore > start) && (keyAfter > start && keyAfter < end)) //里面改里面
                                totalInArrangeKeyChange++;
                        }
                    }
            }

            switch (operation) {
                case 'I':
                    if (gotI) {
                        readLine(mappedByteBuffer, false); //下一行
                        break;
                    }
                    gotI = true;

                    Colume colume = readColume(mappedByteBuffer);
                    System.out.print(colume);
                    System.out.println("-value:" + new String(readArea(mappedByteBuffer, 2)));
                    colume = readColume(mappedByteBuffer);
                    System.out.print(colume);
                    System.out.println("-value:" + new String(readArea(mappedByteBuffer, 2)));
                    colume = readColume(mappedByteBuffer);
                    System.out.print(colume);
                    byte[] sex = readArea(mappedByteBuffer, 2);
                    System.out.print(new String(sex));
                    System.out.println(Arrays.equals(sex, new byte[]{-25, -108, -73}) ? 0 : 1); //男：0,女：1

                    colume = readColume(mappedByteBuffer);
                    System.out.print(colume);
                    System.out.println("-value:" + new String(readArea(mappedByteBuffer, 2)));
                    mappedByteBuffer.get(); //跳过最后的"|"
                    break;
                case 'U':
                    //正常情况
                    if (gotU) {
                        readLine(mappedByteBuffer, false);
                        break;
                    }
                    gotU = true;
                    System.out.println("Update:" + readLine(mappedByteBuffer, true));
                    break;
                case 'D':
                    if (gotD) {
                        readLine(mappedByteBuffer, false);
                        break;
                    }
                    gotD = true;
                    System.out.println("Delete:" + readLine(mappedByteBuffer, true));
                    break;
                default:
                    System.out.println(readLine(mappedByteBuffer, true));
            }
        }

        Long endTime = System.currentTimeMillis();

        logger.info(String.format("file(%s) cost: %s, operation num: %s, in arrange: %s, update primary key: %s, out->in primary key: %s, in->in primary key: %s",
                file.getName(), (endTime - startTime), totalOperationForTable, totalOperationForStarAndEnd, totalOperationUpdatePK, totalOutToIn, totalInArrangeKeyChange));
        logger.info("Schema num: {}", schemaSets.size());
        for (String s : schemaSets) {
            logger.info(s);
        }
        logger.info("Table num: {}", tableSets.size());
        for (String s : tableSets) {
            logger.info(s);
        }
    }


    private static Colume readColume(MappedByteBuffer mappedByteBuffer) {
//        List<Byte> list = new ArrayList<>();
//        Byte b;
//        mappedByteBuffer.get(); //偏移一下
//        while (true) {
//            b = mappedByteBuffer.get();
//            if (b != ':'){
//                list.add(b);
//            } else {
//                break;
//            }
//        }
        int position = mappedByteBuffer.position();
        int offset = position + 1;
        mappedByteBuffer.position(position + 1);
        byte b;
        int i = 0; //由于每行数据一般至少会有一定长度的
        do {
            b = mappedByteBuffer.get(offset + i);
            i++;
        } while (b != ':');
        byte[] bytes = new byte[i - 1];
        mappedByteBuffer.get(bytes);
        Colume colume = new Colume();
        colume.setName(new String(bytes));

        mappedByteBuffer.get(); //跳过“：”
        colume.setType((byte) (mappedByteBuffer.get() - 48)); //数据里是ASCII码值
        mappedByteBuffer.get(); //跳过“：”
        colume.setPrimary(mappedByteBuffer.get() - 48 == 1);
        return colume;
    }


    /**
     * 从当前position读取一行数据
     *
     * @param mappedByteBuffer buffer
     * @param needData         是否需要返回数据，设为false可直接到行末并不保存数据，以节省内存空间
     * @return 数据
     */
    private static String readLine(MappedByteBuffer mappedByteBuffer, boolean needData) {
        int offset = mappedByteBuffer.position();
        byte b;
        int i = 0;
        do {
            b = mappedByteBuffer.get(offset + i);
            i++;
        } while (b != '\n');

        if (needData) {
            byte[] bytes = new byte[i - 1];
            mappedByteBuffer.get(bytes);
            mappedByteBuffer.position(offset + i);
            return new String(bytes);
        } else {
            mappedByteBuffer.position(offset + i);
            return null;
        }
    }


    private static int readIntArea(MappedByteBuffer mappedByteBuffer, int num) {
        byte[] res = readArea(mappedByteBuffer, num);
        if (res[0] == 'N') { //NULL
            return -1;
        } else {
            return Integer.valueOf(new String(res));
        }
    }

    private static String readStringArea(MappedByteBuffer mappedByteBuffer, int num) {
        byte[] res = readArea(mappedByteBuffer, num);
        return new String(res);
    }

    /**
     * 读取相对当前position第n个区域的内容
     *
     * @param mappedByteBuffer buffer
     * @param num              相对当前position的第几个区域
     * @return 区域内容
     */
    private static byte[] readArea(MappedByteBuffer mappedByteBuffer, int num) {
        int lastAppearedPosition = 0;
        int currentAppearedPosition = 0;
        int appearedTimes = 0;
        while (true) {
            if (mappedByteBuffer.get() == '|') {
                appearedTimes++;
                if (appearedTimes != 1) { //非第一次出现，记录上一次位置
                    lastAppearedPosition = currentAppearedPosition;
                }
                currentAppearedPosition = mappedByteBuffer.position();
                if (appearedTimes == num + 1) { //到达指定区域
                    mappedByteBuffer.position(lastAppearedPosition);
                    byte[] bytes = new byte[currentAppearedPosition - lastAppearedPosition - 1];
                    mappedByteBuffer.get(bytes);
                    return bytes;
                }
            }
        }
    }
}
