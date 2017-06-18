package com.alibaba.middleware.race.sync;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by autulin on 6/17/17.
 */
public class Test {
    public static void main(String[] args) {
//        List<Integer> list = new ArrayList<>();
//        for (int i = 0; i < 5; i++) {
//            list.add(i);
//        }
//
//        int size = list.size();
//        for (int i = 0; i < list.size(); i++) {
//            if (list.get(i) == 3) {
//                list.remove(i);
//                i--;
//                continue;
//            }
//            System.out.println(i + ":" + list.get(i) + ",size:" + list.size());
//        }
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());
        System.out.println(getRandomFileName());

    }

    public static String getRandomFileName() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("mmssHHSSS");
        Date date = new Date();
        String s = simpleDateFormat.format(date);
        int random = (int) (new Random().nextDouble() * (99999 - 10000 + 1)) + 10000;
        return random + s;
    }

}
