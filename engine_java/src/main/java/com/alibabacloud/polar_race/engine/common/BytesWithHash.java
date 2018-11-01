package com.alibabacloud.polar_race.engine.common;

public class BytesWithHash extends Object{
    public byte[] bytes = new byte[8];
    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i =0; i < 8; i++) {
            hashCode += bytes[i] * (i+1);
        }
        return hashCode;
    }


    @Override
    public boolean equals(Object o) {
        BytesWithHash ob = (BytesWithHash) o;
        for (int i =0; i < 8; i++) {
            if (bytes[i] != ob.bytes[i]) {
                return false;
            }
        }
        return true;
    }

}
