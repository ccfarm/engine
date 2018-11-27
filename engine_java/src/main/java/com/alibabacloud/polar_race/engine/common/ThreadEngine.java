package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.carrotsearch.hppc.LongIntHashMap;

public class ThreadEngine extends Thread{
    byte name;
    public ThreadEngine(byte name) {
        this.name = name;
    }
    @Override
    public void run() {
        try {
            for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE / 2l; i+= Long.MAX_VALUE / 10000l) {
                byte[] key = Util.longToBytes(i);
                byte[] value = new byte[4 * 1024];
                for (int j = 0; j < 8; j++) {
                    value[j] = key[j];
                }
                Test.client.write(key, value);
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
