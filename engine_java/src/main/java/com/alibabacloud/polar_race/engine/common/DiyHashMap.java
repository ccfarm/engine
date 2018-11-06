package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;

public class DiyHashMap  {
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
    private int hash(long key) {
        //System.out.println(""+key+" "+ capacity );
        int tmp = (int) (key % capacity);
        //System.out.println(tmp);
        if (tmp < 0) {
            tmp = capacity + tmp;
        }
        //System.out.println(tmp);
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

    @Override
    public String toString() {
        return "DiyHashMap{" +
                "arr=" + Arrays.toString(arr) +
                ", capacity=" + capacity +
                '}';
    }
}
