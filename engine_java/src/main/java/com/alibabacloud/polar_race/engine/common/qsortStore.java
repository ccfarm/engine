package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class qsortStore {
    public ConcurrentLinkedQueue<Integer> sig = new ConcurrentLinkedQueue<Integer>();
//    byte[] locks = new byte[64000000];
    volatile static int countIo = 0;
    public int size;
    public long[] keys;
    public int[] position;
    final private static int BUFFERSIZE = 100000;
    final private static int PRE = 1000;
    //final private static int BUFFERSIZE = 500;
    long[] bkeys = new long[BUFFERSIZE];
    byte[][] bvalues = new byte[BUFFERSIZE][4096];
    RandomAccessFile[] valueFiles;
    volatile boolean read29;
    //FileChannel[] fileChannels;

    public ExecutorService pool;
    qsortStore(String path) {
        //pool = Executors.newFixedThreadPool(8);
//        for (int i = 0; i < 64000000; i++) {
//            locks[i] = new AtomicInteger(0);
//        }
        size = 0;
        keys = new long[64000000];
        position = new int[64000000];
        valueFiles = new RandomAccessFile[(int)EngineRace.FILENUM];
        //fileChannels = new FileChannel[(int)EngineRace.FILENUM];
        for (int i = 0; i < EngineRace.FILENUM; i++) {
            try {
                valueFiles[i] = new RandomAccessFile(path + "valueFile" + i, "rw");
                //fileChannels[i] = new RandomAccessFile(path + "valueFile" + i, "r").getChannel();
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
            while (bkeys[i % BUFFERSIZE] != keys[i]) {
                Thread.yield();
            }
            while (bkeys[i % BUFFERSIZE] != keys[i]) Thread.yield();
//            if (bkeys[i % BUFFERSIZE] != keys[i]) {
//                synchronized (bvalues[i % BUFFERSIZE]) {
//                    if (bkeys[i % BUFFERSIZE] != keys[i]) {
//                        try {
//                            bvalues[i % BUFFERSIZE].wait();
//                        } catch (Exception e) {
//                        }
//                    }
//                    bvalues[i % BUFFERSIZE].notifyAll();
//                }
//            }
            visitor.visit(Util.longToBytes(keys[i]), bvalues[i % BUFFERSIZE]);
            i += 1;
        }
        System.out.println(Thread.currentThread().getId() + "done" + i);
        //System.out.println(Thread.currentThread().getId() + "countIo" + countIo);
    }

    public void rangeWithOutReadFirst(long l, long r, AbstractVisitor visitor) {

        int i = find(l);
        read29 = true;
        for (int k = 0; k < PRE; k ++) {
            sig.add(k);
        }
        for (int j = 0; j < 8; j++) {
            read29();
        }
        while (i < size && Util.compare(keys[i], r) < 0) {
            if (i + PRE - 1 < size) {
                //locks[i + PRE - 1]=1;
                sig.add(i + PRE - 1);
            }
            while (bkeys[i % BUFFERSIZE] != keys[i]) Thread.yield();
//            if (bkeys[i % BUFFERSIZE] != keys[i]) {
//                synchronized (bvalues[i % BUFFERSIZE]) {
//                    if (bkeys[i % BUFFERSIZE] != keys[i]) {
//                        try {
//                            bvalues[i % BUFFERSIZE].wait();
//                        } catch (Exception e) {
//                        }
//                    }
//                    bvalues[i % BUFFERSIZE].notifyAll();
//                }
//            }
            visitor.visit(Util.longToBytes(keys[i]), bvalues[i % BUFFERSIZE]);
            i += 1;
        }
        read29 = false;
        System.out.println(Thread.currentThread().getId() + "done" + i);
        //System.out.println(Thread.currentThread().getId() + "countIo" + countIo);
    }
    public void range(long l, long r, AbstractVisitor visitor) {
        long start = System.currentTimeMillis();
        int i = find(l);
        int j = 0;
        while (i < size && Util.compare(keys[i], r) < 0) {
            if (j == 0) {
                for (int k = 0; k < PRE; k ++) {
                    pool.execute(new readT(i + k));
                }
            } else if (i + PRE - 1 < size) {
                //System.out.println((i+63) + "size");
                pool.execute(new readT(i + PRE - 1));
            } else if (i + PRE - 1 == size) {
                //System.out.println((i+63) + "size");
                pool.execute(new readT(-1));
            }
            while (bkeys[i % BUFFERSIZE] != keys[i]) {
                Thread.yield();
            }
            if (j < 10) {
                Util.printBytes(Util.longToBytes(bkeys[i % BUFFERSIZE]));
                Util.printBytes(bvalues[i % BUFFERSIZE]);
            }
            visitor.visit(Util.longToBytes(bkeys[i % BUFFERSIZE]), bvalues[i % BUFFERSIZE]);
            //System.out.println(i);
            i += 1;
            j += 1;
//            if (countIo == 32000000) {
//                System.out.println("rangeExit: " + (System.currentTimeMillis() - start));
//                System.exit(-1);
//            }
        }
        System.out.println("countIo" + countIo);
    }

//    private void read(int i) {
//        int roof = i + 128;
//        while (i < size && i < roof) {
//            if (bkeys[i % BUFFERSIZE] != keys[i]) {
//                if (locks[i % BUFFERSIZE].incrementAndGet() == 1) {
//                    countIo += 1;
//                    long tmpPos = position[i];
//                    tmpPos <<= 12;
//                    int fileIndex = (int) (keys[i] % EngineRace.FILENUM);
//                    if (fileIndex < 0) {
//                        fileIndex += EngineRace.FILENUM;
//                    }
//                    try {
//                        synchronized (valueFiles[fileIndex]) {
//                            valueFiles[fileIndex].seek(tmpPos);
//                            valueFiles[fileIndex].read(bvalues[i % BUFFERSIZE]);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    bkeys[i % BUFFERSIZE] = keys[i];
//                    locks[i % BUFFERSIZE].decrementAndGet();
//                    return;
//                } else {
//                    locks[i % BUFFERSIZE].decrementAndGet();
//                }
//            }
//            i += 1;
//        }
//    }

    public void readAll() {
        (new ReadAll()).start();
    }

    class readT extends Thread {
        private int i;
        readT(int i) {
            this.i = i;
        }
        @Override
        public void run(){
            //System.out.println(i + "run");
            countIo += 1;
            long tmpPos = position[i];
            tmpPos <<= 12;
            int fileIndex = (int) (keys[i] % EngineRace.FILENUM);
            if (fileIndex < 0) {
                fileIndex += EngineRace.FILENUM;
            }
            try {
//                synchronized (valueFiles[fileIndex]) {
//                    valueFiles[fileIndex].seek(tmpPos);
//                    valueFiles[fileIndex].read(bvalues[i % BUFFERSIZE]);
//                }
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                //fileChannels[fileIndex].read(buffer, tmpPos);
                bvalues[i % BUFFERSIZE] = buffer.array();
            } catch (Exception e) {
                e.printStackTrace();
            }
            bkeys[i % BUFFERSIZE] = keys[i];
            if (i < 100) {
                System.out.println(i);
            }
        }
    }

    void read29() {
        (new Read29()).start();
    }

    class Read29 extends Thread {
        @Override
        public void run() {
            Integer i;
            while (read29) {
                if ((i =sig.poll()) == null) {
                    yield();
                    //System.out.println("i am waiting" + System.currentTimeMillis());
                } else {
                    if (i == -1) break;
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
                    bkeys[i % BUFFERSIZE] = keys[i];
//                    synchronized (bvalues[i % BUFFERSIZE]) {
//                        bvalues[i % BUFFERSIZE].notifyAll();
//                    }
                    if (i < 100) {
                        System.out.println(i);
                    } else if (i == size - 1) {
                        read29 = false;
                        break;
                    }
                }
            }
        }
    }

    class ReadAll extends Thread {
        @Override
        public void run() {
            Integer i;
            while (true) {
                if ((i =sig.poll()) == null) {
                    yield();
                } else {
                    if (i == -1) break;
                    try {
                        pool.execute(new readT(i));
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
//            for (int i = 0; i < size; i++) {
//                while (locks[i] == 0) yield();
//                try {
//                    pool.execute(new readT(i));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    break;
//                }
//            }
        }
    }

}