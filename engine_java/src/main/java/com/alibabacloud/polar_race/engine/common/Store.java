package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class Store{
    int BLOCKS = 1;
    int count = 0;
    String path;
    HashMap<Long, Long>  start = new HashMap<Long, Long>();
    RandomAccessFile valueFile;
    RandomAccessFile keyFile;
    boolean readyForRead = false;
    boolean readyForWrite = false;
    public static Store store = new Store();


    synchronized public void start(String path) throws Exception {
        if (this.path == null) {
            this.path = path;
            File curDir = new File(path);
            if (!curDir.exists()) {
                curDir.mkdirs();
            }
            keyFile = new RandomAccessFile(this.path + "keyFile.data", "rw");
            valueFile = new RandomAccessFile(this.path + "valueFile.data", "rw");
        }
    }

    synchronized public void readyForRead() throws Exception{
        if (!readyForRead) {
            int length = (int)keyFile.length();
            byte[] bytes = new byte[length];
            keyFile.read(bytes);
            int i = 0;
            while (i < length) {
                long tmpKey = 0;
                for (int j = 0; j < 8; j++) {
                    tmpKey <<= 8;
                    tmpKey |= (bytes[i + j] & 0xff);
                }
                long tmpPos = 0;
                for (int j = 8; j < 16; j++) {
                    tmpPos <<= 8;
                    tmpPos |= (bytes[i + j] & 0xff);
                }
                start.put(tmpKey, tmpPos);
                i += 16;
            }
            readyForRead = true;
        }
    }

    synchronized public void readyForWrite() throws Exception{
        if (!readyForWrite) {
            keyFile.seek(keyFile.length());
            valueFile.seek(valueFile.length());
            readyForWrite = true;
        }
    }

    public void write(byte[] _key, byte[] value) throws Exception {
        synchronized (this) {
            count += 1;
        }


        synchronized (valueFile) {
            if (!readyForWrite) {
                readyForWrite();
            }
            long pos = valueFile.length();
            valueFile.write(value);
            byte[] newKey = new byte[16];
            for (int i = 0; i < 8; i++) {
                newKey[i] = _key[i];
            }
            for (int i = 0; i < 8; i++) {
                int offset = 64 - (i + 1) * 8;
                newKey[8 + i] = (byte) ((pos >> offset) & 0xff);
            }
            synchronized (keyFile) {
                keyFile.write(newKey);
            }
        }
    }
    public byte[] read(byte[] key) throws Exception{
        synchronized (this) {
            if (!readyForRead) {
                readyForRead();
            }
        }
        long tmpKey = 0;
        for (int i = 0; i < 8; i++) {
            tmpKey <<= 8;
            tmpKey |= (key[i] & 0xff);
        }
        //System.out.println(tmpKey);
        //System.out.println(start);
        long tmpPos = start.get(tmpKey);
        //System.out.println(tmpPos);
        byte[] value = new byte[4 * 1024];
        //System.out.println(2);
        synchronized (valueFile){
            valueFile.seek(tmpPos);
            valueFile.read(value);
        }
        //System.out.println(2);
        return value;
    }
}
