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
    final private static int BUFFERSIZE = 100000;
    //final private static int BUFFERSIZE = 500;
    Entry[] buffer = new Entry[BUFFERSIZE];
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
    public void rangeWithOutRead(long l, long r, AbstractVisitor visitor) {
        int i = find(l);
        while (i < size && Util.compare(keys[i], r) < 0) {
            while (buffer[i % BUFFERSIZE] == null || buffer[i % BUFFERSIZE].key != keys[i]) Thread.yield();
            synchronized (buffer[i % BUFFERSIZE]) {
                if (buffer[i % BUFFERSIZE].key != keys[i]) {
                    try {
                        buffer[i % BUFFERSIZE].wait();
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                buffer[i % BUFFERSIZE].notifyAll();
            }
            visitor.visit(buffer[i % BUFFERSIZE]._key, buffer[i % BUFFERSIZE].value);
            i += 1;
            //System.out.println(Thread.currentThread().getId() + "done" + i);
        }

    }
    public void range(long l, long r, AbstractVisitor visitor) {
        long start = System.currentTimeMillis();
        int i = find(l);
        long timeBeging;
        long timeCost0 = 0;
        long timeCost1 = 0;
        long timeCost2 = 0;
        long timeCost3 = 0;
        while (i < size && Util.compare(keys[i], r) < 0) {
            timeBeging = System.currentTimeMillis();
            countIo += 1;
            long tmpPos = position[i];
            tmpPos <<= 12;
            int fileIndex = (int) (keys[i] % EngineRace.FILENUM);
            if (fileIndex < 0) {
                fileIndex += EngineRace.FILENUM;
            }
            timeCost0 += (timeBeging - System.currentTimeMillis());
            timeBeging = System.currentTimeMillis();
            try {
                valueFiles[fileIndex].seek(tmpPos);
                timeCost1 += (timeBeging - System.currentTimeMillis());
                timeBeging = System.currentTimeMillis();
                if (buffer[i % BUFFERSIZE] == null) {
                    buffer[i % BUFFERSIZE] = new Entry();
                }
                valueFiles[fileIndex].read(buffer[i % BUFFERSIZE].value);
            } catch (Exception e) {
                e.printStackTrace();
            }
            timeCost2 += (timeBeging - System.currentTimeMillis());
            timeBeging = System.currentTimeMillis();
            buffer[i % BUFFERSIZE]._key = Util.longToBytes(keys[i]);
            buffer[i % BUFFERSIZE].key = keys[i];
            synchronized (buffer[i % BUFFERSIZE]) {
                buffer[i % BUFFERSIZE].notifyAll();
            }
            visitor.visit(buffer[i % BUFFERSIZE]._key, buffer[i % BUFFERSIZE].value);
            i += 1;
            if (countIo == 32000000) {
                System.out.println("rangeExit: " + (start - System.currentTimeMillis()));
                System.out.println(timeCost0);
                System.out.println(timeCost1);
                System.out.println(timeCost2);
                System.out.println(timeCost3);
                System.exit(-1);
            }
            timeCost3 += (timeBeging - System.currentTimeMillis());
        }
        System.out.println("countIo" + countIo);
    }
}