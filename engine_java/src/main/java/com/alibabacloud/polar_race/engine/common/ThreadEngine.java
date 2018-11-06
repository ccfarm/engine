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
            client.open("data/");
            for (int i = 0; i < 10000; i++) {
                byte[] key = new byte[8];
                byte[] value = new byte[4 * 1024];
                key[0] = (byte)i;
                value[0] = (byte)i;
                value[1] = name;
                client.write(key, value);
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
