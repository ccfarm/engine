package com.alibabacloud.polar_race.engine.common;

public class ThreadEx extends Thread{
    @Override
    public void run() {
        try {
            Thread.sleep(1000 * 60 * 30);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(-1);
    }
}
