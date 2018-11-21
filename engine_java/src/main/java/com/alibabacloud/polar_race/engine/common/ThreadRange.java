package com.alibabacloud.polar_race.engine.common;

public class ThreadRange extends Thread{
    byte name;
    public ThreadRange(byte name) {
        this.name = name;
    }
    @Override
    public void run() {
        try {
            TestRange.client.range(null, null, new Visitor());
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}