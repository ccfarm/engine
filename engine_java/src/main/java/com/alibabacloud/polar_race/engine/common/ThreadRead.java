package com.alibabacloud.polar_race.engine.common;

public class ThreadRead extends Thread{
    byte name;
    public ThreadRead(byte name) {
        this.name = name;
    }
    @Override
    public void run() {
        try {
            for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE / 2l; i+= Long.MAX_VALUE / 10000l) {
                byte[] key = Util.longToBytes(i);
                byte[] value;
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
            TestRead.client2.close();
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
