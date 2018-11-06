package com.alibabacloud.polar_race.engine.common;

public class TestDHM {
    public static void main(String[] args) {
        DiyHashMap map = new DiyHashMap(64000000);
        for (long i=Long.MIN_VALUE; i < Long.MAX_VALUE/2; i += 99952454325435l) {
            map.put(i, i);
            System.out.println(i);
        }
        for (long i=Long.MIN_VALUE; i < Long.MAX_VALUE/2; i += 99952454325435l) {
            System.out.println(i);
            System.out.println(map.get(i+1));
            if (i != map.get(i)) {
                System.out.println(i == map.get(i));
            }
        }
    }
}
