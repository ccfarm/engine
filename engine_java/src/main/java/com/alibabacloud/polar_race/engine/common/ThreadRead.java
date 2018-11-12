package com.alibabacloud.polar_race.engine.common;

public class ThreadRead extends Thread{
    byte name;
    public ThreadRead(byte name) {
        this.name = name;
    }
    @Override
    public void run() {
        try {
            for (int i = 10000000; i < 10001000; i++) {
                byte[] key = new byte[8];
                byte[] value;
                int mod = 10000000;
                for (int j = 0; j < 8; j++) {
                    key[j] = (byte)(i / mod % 10);
                    mod /= 10;
                }
                value = TestRead.client2.read(key);
                for (int j = 0; j < 8; j++) {
                    if (key[j] != value[j]) {
                        for (int k = 0; k < 8; k++) {
                            System.out.print(key[k]);
                        }
                        System.out.println("");
                        for (int k = 0; k < 8; k++) {
                            System.out.print(value[k]);
                        }
                        System.out.println("");
                        break;
                    }
                }
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
