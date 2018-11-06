package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.RandomAccessFile;

//import com.carrotsearch.hppc.LongLongHashMap;

public class Store{
//    int BLOCKS = 1;
//    int count = 0;
    String path;
    DiyHashMap start;
    RandomAccessFile valueFile;
    RandomAccessFile keyFile;
    boolean readyForRead = false;
    boolean readyForWrite = false;
    public static Store store = new Store();


    synchronized public void start(String path){
        if (this.path == null) {
            this.path = path;
            start = new DiyHashMap(64000000);
            File curDir = new File(path);
            if (!curDir.exists()) {
                curDir.mkdirs();
            }
            try {
                keyFile = new RandomAccessFile(this.path + "keyFile.data", "rw");
                valueFile = new RandomAccessFile(this.path + "valueFile.data", "rw");
            } catch (Exception e) {
                System.out.println(e);
            }
            System.out.println("------");
            System.out.println("start");
            System.out.println("------");
        }
    }

    synchronized public void readyForRead() throws Exception{
        if (!readyForRead) {
            int length = (int)keyFile.length();
            byte[] bytes = new byte[4096];
            int len;
            int i = 0;
            while (i < length) {
                if (length - i >= 4095) {
                    len = 4096;
                } else {
                    len = length - i + 1;
                }
                keyFile.read(bytes, 0, len);
                i += len;
                int j = 0;
                while (j < len) {
                    long tmpKey = 0;
                    for (int k = 0; k < 8; k++) {
                        tmpKey <<= 8;
                        tmpKey |= (bytes[j + k] & 0xff);
                    }
                    long tmpPos = 0;
                    for (int k = 8; k < 16; k++) {
                        tmpPos <<= 8;
                        tmpPos |= (bytes[j + k] & 0xff);
                    }
                    start.put(tmpKey, tmpPos);
                    j += 16;
                }
            }
            readyForRead = true;
            System.out.println("------");
            System.out.println("readyForRead");
            System.out.println("------");
        }
    }

    synchronized public void readyForWrite() throws Exception{
        if (!readyForWrite) {
            keyFile.seek(keyFile.length());
            valueFile.seek(valueFile.length());
            readyForWrite = true;
            System.out.println("------");
            System.out.println("readyForWrite");
            System.out.println("------");
        }
    }

    public void write(byte[] _key, byte[] value) throws Exception {


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
