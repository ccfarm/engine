package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class qsortStore {
    AtomicInteger[] locks = new AtomicInteger[BUFFERSIZE];
    static int countIo = 0;
    public int size;
    public long[] keys;
    public int[] position;
    final private static int BUFFERSIZE = 150000;
    //final private static int BUFFERSIZE = 50;
    long[] bkeys = new long[BUFFERSIZE];
    byte[][] bvalues = new byte[BUFFERSIZE][4096];
    RandomAccessFile[] valueFiles;
    qsortStore(String path) {
        for (int i = 0; i < BUFFERSIZE; i++) {
            locks[i] = new AtomicInteger();
        }
        size = 0;
        keys = new long[64000000];
        position = new int[64000000];
        valueFiles = new RandomAccessFile[(int)EngineRace.FILENUM];
        for (int i = 0; i < EngineRace.FILENUM; i++) {
            try {
                valueFiles[i] = new RandomAccessFile(path + "valueFile" + i, "rw");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void put(long key, int position) {
        keys[size] = key;
        this.position[size] = position;
        size += 1;
    }
    public void sort() {
        long start = System.currentTimeMillis();
        qsort(0, size-1);
        System.out.println("sortCost: " + (System.currentTimeMillis() - start));
    }
    private void qsort(int l, int r) {
        int i = l;
        int j = r;
        long t1 = keys[i];
        int t2 = position[i];
        while (i < j) {
            while (Util.compare(keys[j], t1) >= 0 && i < j) {
                j -= 1;
            }
            if (i < j) {
                keys[i] = keys[j];
                position[i] = position[j];
                i += 1;
            }
            while (Util.compare(keys[i], t1) <= 0 && i < j) {
                i += 1;
            }
            if (i < j) {
                keys[j] = keys[i];
                position[j] = position[i];
                j -= 1;
            }
        }
        keys[i] = t1;
        position[i] = t2;
        i += 1;
        j -= 1;
        if (i < r) qsort(i, r);
        if (l < j) qsort(l, j);
    }
    private int find(long target) {
        int i = 0;
        int j = size - 1;
        while (i <= j) {
            int m = (i + j) / 2;
            long cmp = Util.compare(keys[m], target);
            if (cmp == 0l) {
                return m;
            } else if (cmp < 0l) {
                i = m + 1;
            } else {
                j = m - 1;
            }
        }
        if (j < 0) {
            return 0;
        } else if (Util.compare(keys[j], target) < 0l) {
            return i;
        } else {
            return j;
        }
    }

    public void range(long l, long r, AbstractVisitor visitor) {
        int i = find(l);
        long last = 0;
        while (i < size && Util.compare(keys[i], r) < 0) {
            if (last == keys[i]) {
                i += 1;
                continue;
            }
            last = keys[i];
            byte[] _key = Util.longToBytes(keys[i]);
            boolean flag = false;
            if (bkeys[i % BUFFERSIZE] != keys[i]) {
                if (locks[i % BUFFERSIZE].getAndIncrement() == 0) {
                    flag = true;
                    countIo += 1;
                    bkeys[i % BUFFERSIZE] = keys[i];
                    long tmpPos = position[i];
                    tmpPos <<= 12;
                    int fileIndex = (int) (keys[i] % EngineRace.FILENUM);
                    if (fileIndex < 0) {
                        fileIndex += EngineRace.FILENUM;
                    }
                    try {
                        valueFiles[fileIndex].seek(tmpPos);
                        valueFiles[fileIndex].read(bvalues[i % BUFFERSIZE]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                locks[i % BUFFERSIZE].getAndDecrement();
            }
            while (locks[i % BUFFERSIZE].get() > 0);
            visitor.visit(_key, bvalues[i % BUFFERSIZE]);

//            try {
//                if (flag) {
//                    //Thread.sleep(1);
//                    //Thread.yield();
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
            i += 1;
        }
        System.out.println("countIo" + countIo);
    }
}
