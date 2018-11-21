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
    public static byte[] longToBytes(long tmp) {
        byte[] key = new byte[8];
        for (int j = 0; j < 8; j++) {
            int offset = 64 - (j + 1) * 8;
            key[j] = (byte) ((tmp >> offset) & 0xff);
        }
        return key;
    }
    public static long compare(long a, long b) {
        if (((a >= 0l) && (b >= 0l)) ||
                ((a < 0l) && (b < 0l))) {
            return a - b;
        } else {
            if (a < 0) {
                return 1l;
            } else {
                return -1l;
            }
        }
    }
    public static void printBytes(byte[] bytes) {
        for (int i = 0; i < 8; i++) {
            System.out.print(bytes[i] + " ");
        }
        System.out.println();
    }
}
