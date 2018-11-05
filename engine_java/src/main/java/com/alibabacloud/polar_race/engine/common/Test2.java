package com.alibabacloud.polar_race.engine.common;

import java.util.ArrayList;

public class Test2 {
    public static void main(String[] args) {
        ArrayList<ThreadEngine> ts = new ArrayList<ThreadEngine>();
        for (int i =32; i < 64; i++) {
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



    }
}
