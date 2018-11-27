package com.alibabacloud.polar_race.engine.common;

public class TestRange {
    public static EngineRace client;
    public static void main(String[] args) {
        client = new EngineRace();
        try {
            client.open("data");
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i =0; i < 4; i++) {
            ThreadRange t1 = new ThreadRange((byte)i);
            t1.start();
        }
        //client2.close();

    }
}
