package com.alibaba.middleware.race.sync;

/**
 * Created by autulin on 6/13/17.
 */
public class Utils {
    public static int bytes2Int(byte[] bytes) {
        return Integer.parseInt(new String(bytes));
    }
}
