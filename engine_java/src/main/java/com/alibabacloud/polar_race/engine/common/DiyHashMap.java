package com.alibabacloud.polar_race.engine.common;

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
        int tmp = (int) (key % (long)capacity);
        if (tmp < 0) {
            tmp = capacity + tmp;
        }
        return tmp;
    }

    public void put(long key, long value) {
        int hash = hash(key);
        Entry entry = new Entry(key, value);
        if (arr[hash] == null) {
            arr[hash] = entry;
        } else {
            Entry tmp = arr[hash];
            while (tmp.next != null) {
                tmp = tmp.next;
            }
            tmp.next = entry;
        }
    }
    public long get(long key) {
        int hash = hash(key);
        Entry tmp = arr[hash];
        while (tmp.key != key) {
            tmp = tmp.next;
        }
        return tmp.value;
    }
}
