package com.alibaba.middleware.race.sync;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by autulin on 6/13/17.
 */
public class Utils {
    public static long bytes2Long(byte[] bytes) {
        return Long.parseLong(new String(bytes));
    }
    public static final int QUEUE_SIZE = 8192 * 100;
    public static BlockingQueue<Binlog> binlogQueue = new LinkedBlockingQueue<>();
    public static boolean isInRange(long id){
        if(id>Server.startPkId && id<Server.endPkId){
            return true;
        }else{
            return false;
        }
    }
}
