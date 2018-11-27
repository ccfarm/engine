package com.alibabacloud.polar_race.engine.common;

public class Entry {
    public long key;
    public byte[] _key;
    public byte[] value;
    public Entry() {
        this.value = new byte[4096];
    }
    public Entry(long key) {
        this.key = key;
        this._key = Util.longToBytes(key);
        this.value = new byte[4096];
    }
}
