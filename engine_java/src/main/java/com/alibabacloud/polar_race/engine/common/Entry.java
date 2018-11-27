package com.alibabacloud.polar_race.engine.common;

public class Entry {
    volatile public long key;
    volatile public byte[] _key;
    volatile public byte[] value;
    public Entry() {
        this.value = new byte[4096];
    }
    public Entry(long key) {
        this.key = key;
        this._key = Util.longToBytes(key);
        this.value = new byte[4096];
    }
}
