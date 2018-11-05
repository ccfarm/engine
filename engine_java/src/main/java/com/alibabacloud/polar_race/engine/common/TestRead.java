package com.alibabacloud.polar_race.engine.common;

public class TestRead {
    public static void main(String[] args) {
        for (int i =0; i < 64; i++) {
            ThreadRead t1 = new ThreadRead((byte)i);
            t1.start();
        }

    }
}
