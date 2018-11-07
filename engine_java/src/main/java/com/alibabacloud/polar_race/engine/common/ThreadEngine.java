package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.EngineRace;

public class ThreadEngine extends Thread{
    byte name;
    public ThreadEngine(byte name) {
        this.name = name;
    }
    @Override
    public void run() {
        EngineRace client = new EngineRace();
        try {
            client.open("data");
            for (int i = 10000000; i < 10110000; i++) {
                byte[] key = new byte[8];
                byte[] value = new byte[4 * 1024];
                int mod = 10000000;
                for (int j = 0; j < 8; j++) {
                    key[j] = (byte)(i / mod % 10);
                    value[j] = (byte)(i / mod % 10);
                    mod /= 10;
                }
                client.write(key, value);
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
