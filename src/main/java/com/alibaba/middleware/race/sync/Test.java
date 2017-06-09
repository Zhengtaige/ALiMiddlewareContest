package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

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

        System.out.println(readLine(mappedByteBuffer));
        mappedByteBuffer.get(); //跳过“/n”

        boolean getI = false;
        boolean getU = false;
        boolean getD = false;
        for (int i = 0; i < 5; i++) {
            System.out.println("schema:" + new String(readArea(mappedByteBuffer, 3)));
            System.out.println("table:" + new String(readArea(mappedByteBuffer, 1)));
            switch (readArea(mappedByteBuffer, 1)[0]) {
                case 'I':
                    if (getI) {
                        System.out.println(readLine(mappedByteBuffer));
                        break;
                    }
                    getI = true;
                    Colume colume = readColume(mappedByteBuffer);
                    System.out.print(colume);
                    System.out.println("-value:" + new String(readArea(mappedByteBuffer, 2)));
                    colume = readColume(mappedByteBuffer);
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


                    break;
                default:
                    System.out.println(readLine(mappedByteBuffer));
            }
            mappedByteBuffer.get(); // lazy
            System.out.println("------------------------------");
        }





//        while (true) {

       /* System.out.println(readString(mappedByteBuffer, offset, '|'));

        offset = mappedByteBuffer.position() + 1;

        System.out.println(readString(mappedByteBuffer, offset, '|'));

        offset = mappedByteBuffer.position() + 1;
        mappedByteBuffer.position(offset);
        b = mappedByteBuffer.get();
        switch (b) {
            case 'I':
                System.out.println("insert");
                offset += 13;
                int id = mappedByteBuffer.getInt(offset);
                offset += 21;
                String fName = readString(mappedByteBuffer, offset, '|');
                offset += 20;
                String lastName = readString(mappedByteBuffer, offset, '|');
                offset += 14;
                String sex = readString(mappedByteBuffer, offset, '|');
                offset += 16;
                String score = readString(mappedByteBuffer, offset, '|');
                System.out.println(String.format("%s %s %s %s %s", id, fName, lastName,sex,score));
                break;
            case 'U':
                System.out.println("update");
                break;
            case 'D':
                System.out.println("delete");
        }*/

//        }


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


    private static String readLine(MappedByteBuffer mappedByteBuffer) {
        int offset = mappedByteBuffer.position();
        byte b;
        int i = 0; //由于每行数据一般至少会有一定长度的
        do {
            b = mappedByteBuffer.get(offset + i);
            i++;
        } while (b != '\n');
//        mappedByteBuffer.slice()
        byte[] bytes = new byte[i - 1];
        mappedByteBuffer.get(bytes);
        mappedByteBuffer.position(offset + i - 1);
        return new String(bytes);
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
                if (appearedTimes == num + 1) {
                    mappedByteBuffer.position(lastAppearedPosition);
                    byte[] bytes = new byte[currentAppearedPosition - lastAppearedPosition - 1];
                    mappedByteBuffer.get(bytes);
                    return bytes;
                }
            }
        }
    }
}
