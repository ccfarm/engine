package com.alibabacloud.polar_race.engine.common;

public class ThreadRead extends Thread{
    byte name;
    public ThreadRead(byte name) {
        this.name = name;
    }
    @Override
    public void run() {
        EngineRace client2 = new EngineRace();
        try {
            client2.open("data/");
            for (int i = 0; i < 1000; i++) {
                byte[] key = new byte[8];
                byte[] value;
                key[0] = (byte)i;
                value = client2.read(key);
                System.out.println(name + " " +(byte)i + value[0] + " " +value[1]);
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
