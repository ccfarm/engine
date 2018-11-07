package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.RandomAccessFile;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongLongHashMap;

public class Store{
//    int BLOCKS = 1;
//    int count = 0;
    int user = 0;
    String path;
    DiyHashMap start;
    //LongLongHashMap start;
    RandomAccessFile valueFile;
    RandomAccessFile keyFile;
    boolean readyForRead = false;
    boolean readyForWrite = false;
    public static Store store = new Store();


    synchronized public void start(String path){
        if (this.path == null) {
            this.path = path + "/";
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
        }
        user += 1;
        System.out.println("------");
        System.out.println("start");
        System.out.println(user);
        System.out.println("------");
    }

    synchronized public void end() {
        user -= 1;
        System.out.println("------");
        System.out.println("end");
        System.out.println(user);
        System.out.println("------");
//        if (user == 0) {
//            try {
//                keyFile.close();
//                valueFile.close();
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//        }
    }

    synchronized public void readyForRead() {
        if (!readyForRead) {
            try {
                start = new DiyHashMap(128000000);
                //start = new DiyHashMap(3);
                //start = new LongLongHashMap(64000000, 0.99);
                int length = (int) keyFile.length();
                //System.out.println(length);
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
            } catch (Exception e) {
                System.out.println(e);
            }
            System.out.println("------");
            System.out.println("readyForRead");
            System.out.println("------");
        }
    }

    synchronized public void readyForWrite(){
        if (!readyForWrite) {
            try {
                keyFile.seek(keyFile.length());
                valueFile.seek(valueFile.length());
                readyForWrite = true;
            } catch (Exception e) {
                System.out.println(e);
            }
            System.out.println("------");
            System.out.println("readyForWrite");
            System.out.println("------");
        }
    }

    public void write(byte[] _key, byte[] value) {
        if (!readyForWrite) {
            readyForWrite();
        }
        try {
            long pos;
            synchronized (valueFile) {
                pos = valueFile.length();
                valueFile.write(value);
            }
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
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public byte[] read(byte[] key) throws EngineException{
        if (!readyForRead) {
            readyForRead();
        }
        long tmpKey = 0;
        for (int i = 0; i < 8; i++) {
            tmpKey <<= 8;
            tmpKey |= (key[i] & 0xff);
        }
        //System.out.println(tmpKey);
        //System.out.println(start);
        long tmpPos = start.get(tmpKey);
        if (tmpPos == -1l) {
            for (int k = 0; k < 8; k++) {
                System.out.print(key[k]);
            }
            throw new EngineException(RetCodeEnum.NOT_FOUND, "");
        }
        //System.out.println(tmpPos);
        byte[] value = new byte[4 * 1024];
        //System.out.println(2);
        try {
            synchronized (valueFile) {
                valueFile.seek(tmpPos);
                valueFile.read(value);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        //System.out.println(2);
        return value;
    }
}
