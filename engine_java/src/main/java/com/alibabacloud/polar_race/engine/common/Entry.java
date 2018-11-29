package com.alibabacloud.polar_race.engine.common;

public class Entry {
    volatile public long key;
    volatile public byte[] _key;
    volatile public byte[] value;
}
