package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class qsortStore {
    public int size;
    public long[] keys;
    public int[] position;
    final private static int BUFFERSIZE = 1000;
    long[] bkeys = new long[BUFFERSIZE];
    byte[] bcounts = new byte[BUFFERSIZE];
    byte[][] bvalues = new byte[BUFFERSIZE][4096];
    RandomAccessFile[] valueFiles;
    qsortStore(String path) {
        size = 0;
        keys = new long[64000000];
        position = new int[64000000];
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
        qsort(0, size-1);
    }
    private void qsort(int l, int r) {
        int i = l;
        int j = r;
        long t1 = keys[i];
        int t2 = position[i];
        while (i < j) {
            while (keys[j] >= t1 && i < j) {
                j -= 1;
            }
            if (i < j) {
                keys[i] = keys[j];
                position[i] = position[j];
                i += 1;
            }
            while (keys[i] <= t2 && i < j) {
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
    synchronized private boolean waitFor(int i) {
        if (bkeys[i % BUFFERSIZE] != 0 && bcounts[i % BUFFERSIZE] < 64) return false;
        bkeys[i % BUFFERSIZE] = keys[i];
        bcounts[i % BUFFERSIZE] = 0;
        long tmpPos = position[i];
        tmpPos <<= 12;
        int fileIndex = (int) (keys[i] % EngineRace.FILENUM);
        if (fileIndex < 0) {
            fileIndex += EngineRace.FILENUM;
        }
        try {
            synchronized (valueFiles[fileIndex]) {
                valueFiles[fileIndex].seek(tmpPos);
                valueFiles[fileIndex].read(bvalues[i % BUFFERSIZE]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public void range(AbstractVisitor visitor) {
        for (int i = 0; i < size; i++) {
            byte[] _key = new byte[8];
            if (bkeys[i % BUFFERSIZE] != keys[i]) {
                while (!waitFor(i)) ;
            }
            for (int j = 0; j < 8; i++) {
                int offset = 64 - (j + 1) * 8;
                _key[i] = (byte) ((keys[i] >> offset) & 0xff);
            }
            visitor.visit(_key, bvalues[i % BUFFERSIZE]);
            synchronized (this) {
                bcounts[i % BUFFERSIZE] += 1;
            }
        }
    }
}
