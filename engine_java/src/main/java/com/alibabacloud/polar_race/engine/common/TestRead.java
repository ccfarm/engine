package com.alibabacloud.polar_race.engine.common;

public class TestRead {
    public static EngineRace client2;
    public static void main(String[] args) {
        client2 = new EngineRace();
        try {
            client2.open("data");
        } catch (Exception e) {
            System.out.println(e);
        }
        for (int i =0; i < 3; i++) {
            ThreadRead t1 = new ThreadRead((byte)i);
            t1.start();
        }
        //client2.close();

    }
}
