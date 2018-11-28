package com.alibabacloud.polar_race.engine.common;

import java.util.ArrayList;

public class TestRange {
    public static EngineRace client;
    public static void main(String[] args) {
        client = new EngineRace();
        try {
            client.open("data");
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<ThreadRange> ts = new ArrayList<ThreadRange>();
        for (int i =0; i < 4; i++) {
            ThreadRange t1 = new ThreadRange((byte)i);
            t1.start();
            ts.add(t1);
        }
        for( int i = 0 ; i < ts.size() ; i++) {
            try {
                ts.get(i).join();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        client.close();
    }
}
