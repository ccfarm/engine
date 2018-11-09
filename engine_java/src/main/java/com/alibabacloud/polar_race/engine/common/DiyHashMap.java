package com.alibabacloud.polar_race.engine.common;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.Arrays;

public class DiyHashMap  {
    HashFunction hashFunction = Hashing.murmur3_32();
    private class Entry {
        long key;
        long value;
        Entry next;
        Entry(long key, long value) {
            this.key = key;
            this.value = value;
            next = null;
        }
    }
    Entry[] arr;
    int capacity;
    DiyHashMap(int capacity) {
        this.capacity = capacity;
        arr = new Entry[capacity];
    }

    public static int _hash(final byte[] data) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = 0x9747b28c^8;

        for (int i=0; i<2; i++) {
            final int i4 = i*4;
            int k = (data[i4+0]&0xff) +((data[i4+1]&0xff)<<8)
                    +((data[i4+2]&0xff)<<16) +((data[i4+3]&0xff)<<24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    private int hash(long key) {

        //System.out.println(""+key+" "+ capacity );
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((key >> offset) & 0xff);
        }
        int tmp = _hash(buffer);
        //System.out.println(tmp);
        tmp %= capacity;
        //System.out.println(tmp);
        if (tmp < 0) {
            tmp = capacity + tmp;
        }
        //System.out.println(key);
        System.out.println(tmp);
        return tmp;
    }

    public void put(long key, long value) {
        int hash = hash(key);
        Entry entry = new Entry(key, value);
        entry.next = arr[hash];
        arr[hash] = entry;
    }

    public long get(long key) {
        int hash = hash(key);
        Entry tmp = arr[hash];
        while (tmp != null && tmp.key != key) {
            tmp = tmp.next;
        }
        if (tmp != null) {
            return tmp.value;
        } else {
            return -1l;
        }

    }
}
