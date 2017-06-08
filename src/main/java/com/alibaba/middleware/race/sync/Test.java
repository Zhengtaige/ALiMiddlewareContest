package com.alibaba.middleware.race.sync;

/**
 * Created by autulin on 6/7/17.
 */
public class Test {
    public static void main(String[] args) {
        String[] ags = new String[]{"1", "2", "3", "4"};
        String s = String.format("%s,%s,%s,%s", ags);
        System.out.println(s);
    }
}
