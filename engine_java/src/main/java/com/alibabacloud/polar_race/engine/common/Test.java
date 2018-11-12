package com.alibabacloud.polar_race.engine.common;

import java.util.ArrayList;

public class Test {
    public static EngineRace client;

    public static void main(String[] args) {
        client = new EngineRace();
        try {
            client.open("data");
        } catch (Exception e) {
            System.out.println(e);
        }
        ArrayList<ThreadEngine> ts = new ArrayList<ThreadEngine>();
        for (int i =0; i < 8; i++) {
            ThreadEngine t1 = new ThreadEngine((byte)i);
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
