package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.ResultMap;


public class MyRunnable implements Runnable {
    ResultMap resultMap;

    public MyRunnable(ResultMap resultMap) {
        this.resultMap = resultMap;
    }

    @Override
    public void run() {

    }
}
