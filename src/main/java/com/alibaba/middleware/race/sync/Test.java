package com.alibaba.middleware.race.sync;

/**
 * Created by autulin on 6/17/17.
 */
public class Test {
    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        byte[][][] resultmap = new byte[7000000][][];
        System.out.println(resultmap.length);
        for (int i = 0; i < resultmap.length; i++) {
            resultmap[i] = new byte[5][];
            for (int j = 0; j < 5; j++) {
                resultmap[i][j] = new byte[]{1, 2, 3};
            }
        }
        System.out.println(System.currentTimeMillis());
        System.out.println(resultmap[1234][2][1]);
        System.out.println(System.currentTimeMillis());
    }
}
