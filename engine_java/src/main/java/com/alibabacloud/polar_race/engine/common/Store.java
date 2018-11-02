package com.alibabacloud.polar_race.engine.common;

import java.io.RandomAccessFile;
import java.util.HashMap;

public class Store{
    int BLOCKS = 256;
    int count = 0;
    HashMap<BytesWithHash, Long>  start = new HashMap<BytesWithHash, Long>();
    HashMap<Integer, RandomAccessFile> valueMap = new HashMap<Integer, RandomAccessFile>();
    String path;
    RandomAccessFile keyFile;
    boolean readyForRead = false;
    public static Store store = new Store();


    synchronized public void start(String path) throws Exception {
        if (store.path == null) {
            store.path = path;
            store.keyFile = new RandomAccessFile(store.path + "keyFile.data", "rw");
        }
    }

    synchronized public void readyForRead() throws Exception{
        if (!this.readyForRead) {
            BytesWithHash key = new BytesWithHash();
            while (store.keyFile.read(key.bytes) != -1) {
                byte[] bytes = new byte[8];
                store.keyFile.read(bytes);
                long tmpLong = 0;
                for (int i = 0; i < 8; i++) {
                    tmpLong <<= 8;
                    tmpLong |= (bytes[i] & 0xff);
                }
                store.start.put(key, tmpLong);
                key = new BytesWithHash();
            }
        }
    }


    private int hash(BytesWithHash key) {
        return key.hashCode() % BLOCKS;
    }


    public void write(byte[] _key, byte[] value) throws Exception {
        synchronized (this) {
            count += 1;
        }
        BytesWithHash key = new BytesWithHash();
        key.bytes = _key;
        int keyHash = hash(key);
        if (!valueMap.containsKey(keyHash)) {
            RandomAccessFile f = new RandomAccessFile(store.path + keyHash + ".data", "rw");
            f.seek(f.length());
            valueMap.put(keyHash, f);
        }
        RandomAccessFile f = valueMap.get(keyHash);
        synchronized (f) {
            //start.put(key, f.length());
//            System.out.println(key.bytes[0]);
//            System.out.println(key.hashCode());
            f.write(value);
            byte[] newKey = new byte[16];
            for (int i = 0; i < 8; i++) {
                newKey[i] = _key[i];
            }
            for (int i = 0; i < 8; i++) {
                int offset = 64 - (i + 1) * 8;
                newKey[8 + i] = (byte) ((f.length() >> offset) & 0xff);
            }
            synchronized (keyFile) {
                keyFile.write(newKey);
            }
        }
    }
    public byte[] read(byte[] _key) throws Exception{
        if (!readyForRead) {
            readyForRead();
        }
        BytesWithHash key = new BytesWithHash();
        key.bytes = _key;
        int keyHash = hash(key);
        if (!valueMap.containsKey(keyHash)) {
            RandomAccessFile f = new RandomAccessFile(store.path + keyHash + ".data", "rw");
            valueMap.put(keyHash, f);
        }
        RandomAccessFile f = valueMap.get(keyHash);
        //System.out.println(start.containsKey(key));
//        System.out.println(key.bytes[0]);
//        System.out.println(key.hashCode());
        long tmpLong = start.get(key);
        byte[] value = new byte[4 * 1024];
        synchronized (f){
            f.seek(tmpLong);
            f.read(value);
        }
        return value;
    }
}
