package com.alibabacloud.polar_race.engine.common;

public class DiyAVL {
    private class Entry {
        long key;
        int value;
        Entry left;
        Entry right;
        Entry(long key, int value) {
            this.key = key;
            this.value = value;
            left = null;
            right = null;
        }
    }
    Entry root;
    private long compare(long a, long b) {
        if (((a >= 0l) && (b >= 0l)) ||
                        ((a < 0l) && (b < 0l))) {
            return a - b;
        } else {
            if (a < 0) {
                return -1l;
            } else {
                return 1l;
            }
        }


    }
    public void put(long key, int value) {
        if (root == null) {
            root = new Entry(key, value);
        }
        Entry tmp = root;
        while (true) {
            long re = compare(key, tmp.key);
            if (re == 0l) {
                tmp.value = value;
                return;
            } else if (re < 0l) {
                if (tmp.left != null) {
                    tmp = tmp.left;
                } else {
                    tmp.left = new Entry(key, value);
                    return;
                }
            } else {
                if (tmp.right != null) {
                    tmp = tmp.right;
                } else {
                    tmp.right = new Entry(key, value);
                    return;
                }
            }
        }
    }

    public int get(long key) {
        Entry tmp = root;
        while (tmp != null) {
            long re = compare(key, tmp.key);
            if (re == 0l) {
                return tmp.value;
            } else if (re < 0l) {
                tmp = tmp.left;
            } else {
                tmp = tmp.right;
            }
        }
        return -1;
    }
}
