package com.alibaba.middleware.race.sync;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by autulin on 6/13/17.
 */
public class Utils {
    public static int bytes2Int(byte[] bytes) {
        return Integer.parseInt(new String(bytes));
    }
    public static BlockingQueue<Binlog> binlogQueue = new LinkedBlockingQueue<>();


}
