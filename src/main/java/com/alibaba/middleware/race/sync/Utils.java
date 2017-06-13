package com.alibaba.middleware.race.sync;

/**
 * Created by autulin on 6/13/17.
 */
public class Utils {
    public static long bytes2Long(byte[] bytes) {
        return Long.parseLong(new String(bytes));
    }
}
