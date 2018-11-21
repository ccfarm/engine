package com.alibabacloud.polar_race.engine.common;

public class Util {
    public static long bytesToLong(byte[] bytes) {
        long tmp = 0;
        for (int i = 0; i < 8; i++) {
            tmp <<= 8;
            tmp |= (bytes[i] & 0xff);
        }
        return tmp;
    }
}
