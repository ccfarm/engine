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
            client2.open("data");
            for (int i = 10000000; i < 10110000; i++) {
                byte[] key = new byte[8];
                byte[] value;
                int mod = 10000000;
                for (int j = 0; j < 8; j++) {
                    key[j] = (byte)(i / mod % 10);
                    mod /= 10;
                }
                value = client2.read(key);
                for (int j = 0; j < 8; j++) {
                    if (key[j] != value[j]) {
                        for (int k = 0; k < 8; k++) {
                            System.out.print(key[k]);
                        }
                        for (int k = 0; k < 8; k++) {
                            System.out.print(value[k]);
                        }
                        break;
                    }
                }
            }
            client2.close();
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
