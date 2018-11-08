package com.alibabacloud.polar_race.engine.common;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

public class TestMM {
    public static void main(String[] args) {
        try {
            RandomAccessFile f = new RandomAccessFile("data/1.data", "rw");
            System.out.println(f.getFilePointer());


            FileChannel chan = f.getChannel();
            MappedByteBuffer buff = chan.map(FileChannel.MapMode.READ_WRITE, 0,  48);
            byte[] bytes = new byte[4];
            bytes[0] = (byte)1;
            buff.put(bytes);
            System.out.println(f.getFilePointer());
            buff.position(0);
            bytes = new byte[48];
            buff.get(bytes);



            for (int i =0; i<48; i++) {
                System.out.print(bytes[i]);
            }
        } catch (Exception e) {

        }
    }
}
