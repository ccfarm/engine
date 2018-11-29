package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {
	public AtomicInteger lock = new AtomicInteger(0);
	final static long MAPSIZE = 12l * 64 * 1000 * 1000;
	//final static long FILESIZE =  4 * 1024 * 1024 * 1024;
    final static long FILENUM =  1024;
	String path;
	//DiyHashMap position;
	//LongLongHashMap position;
    LongIntHashMap position;
	//AVLTree position;
	//BPlusTree position;
	RandomAccessFile keyFile;
	MappedByteBuffer buffKeyFile;
	//MappedByteBuffer buffValueFile;
	boolean readyForRead = false;
	boolean readyForWrite = false;
	boolean readyForRange = false;
	//FileChannel channelKeyFile;
	long countKeyFile = 0l;
	//long countValueFile = 0l;
	RandomAccessFile[] valueFiles;
	qsortStore qsortStore;
	long threadId = -1;
	long start;
	@Override
	public void open(String path) throws EngineException {
		this.start = System.currentTimeMillis();
		this.path = path + "/";
		File curDir = new File(path);
		if (!curDir.exists()) {
			curDir.mkdirs();
		}
//		try {
//			keyFile = new RandomAccessFile(this.path + "keyFile.data", "rw");
//			valueFile = new RandomAccessFile(this.path + "valueFile.data", "rw");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		System.out.println("------");
		System.out.println("start");
		System.out.println("------");

	}

	synchronized public void readyForRead() {
		if (!readyForRead) {
            long start = System.currentTimeMillis();
			try {
				keyFile = new RandomAccessFile(this.path + "keyFile", "r");
				//channelKeyFile = keyFile.getChannel();
				valueFiles = new RandomAccessFile[(int)FILENUM];
				for (int i = 0; i < FILENUM; i++) {
				    valueFiles[i] = new RandomAccessFile(this.path + "valueFile" + i, "rw");
                }
				//valueFile = new RandomAccessFile(this.path + "valueFile.data", "r");
				//position = new DiyHashMap(64000000);
				//position = new DiyHashMap(3);
				position = new LongIntHashMap(64000000, 0.99);
				//position = new BPlusTree();
                //position = new LongLongHashMap();
				int length = (int) keyFile.length();
				//System.out.println(length);
                //ByteBuffer tmpBuffer;
				byte[] bytes = new byte[3 * 4 * 1024];
				int len = 3 * 4 * 1024;
				int i = 0;
				while (i < length) {
                    //tmpBuffer = ByteBuffer.allocate(len);
					//channelKeyFile.read(tmpBuffer);
                    keyFile.read(bytes);
                    //bytes = tmpBuffer.array();
					i += 3 * 4 * 1024;
					int j = 0;
					while (j < len) {
						long tmpKey = 0;
						for (int k = 0; k < 8; k++) {
							tmpKey <<= 8;
							tmpKey |= (bytes[j + k] & 0xff);
						}
						int posInt = 0;
						for (int k = 8; k < 12; k++) {
							posInt <<= 8;
                            posInt |= (bytes[j + k] & 0xff);
						}
                        if (tmpKey == 0 && posInt == 0) break;
						position.put(tmpKey, posInt);
						j += 12;
					}
				}



				keyFile = new RandomAccessFile(this.path + "keyFilePlus", "rw");
                buffKeyFile = keyFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, MAPSIZE);
                java.util.Iterator< LongIntCursor >  iter = position.iterator();
                for (LongIntCursor kv : position) {
                    byte[] key = Util.longToBytes(kv.key);
                    byte[] newKey = new byte[12];
                    for (int j = 0; j < 8; j++) {
                        newKey[j] = key[j];
                    }
                    for (int j = 0; j < 4; j++) {
                        int offset = 32 - (j + 1) * 8;
                        newKey[8 + j] = (byte) ((kv.value >>> offset) & 0xff);
                    }
                    buffKeyFile.put(newKey);
                }
				readyForRead = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
            System.out.println("readyForReadCost: " + (System.currentTimeMillis() - start));
		}
	}

	synchronized public void readyForWrite(){
		if (!readyForWrite) {
            long start = System.currentTimeMillis();
			try {
				keyFile = new RandomAccessFile(this.path + "keyFile", "rw");
				countKeyFile = 0;
				if (keyFile.length() == MAPSIZE) {
                    byte[] bytes = new byte[(int) MAPSIZE];
                    keyFile.read(bytes);
                    while (countKeyFile < MAPSIZE) {
                        boolean flag = true;
                        for (int i = 0; i < 12; i++) {
                            if (bytes[(int) countKeyFile + i] != 0) {
                                flag = false;
                            }
                        }
                        if (flag) {
                            break;
                        }
                        countKeyFile += 12l;
                    }
                }
                buffKeyFile = keyFile.getChannel().map(FileChannel.MapMode.READ_WRITE, countKeyFile, MAPSIZE - countKeyFile);
				//channel = keyFile.getChannel();
                valueFiles = new RandomAccessFile[(int)FILENUM];
                for (int i = 0; i < FILENUM; i++) {
                    valueFiles[i] = new RandomAccessFile(this.path + "valueFile" + i, "rw");
                    valueFiles[i].seek(valueFiles[i].length());
                }
				readyForWrite = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
            System.out.println("readyForWriteCost: " + (System.currentTimeMillis() - start));
		}
	}

	synchronized public void readyForRange() {
	    if (!readyForRange) {
            long start = System.currentTimeMillis();
	        try {
                qsortStore = new qsortStore(path);
                keyFile = new RandomAccessFile(this.path + "keyFilePlus", "r");
                int length = (int) keyFile.length();
                byte[] bytes = new byte[3 * 4 * 1024];
                int len = 3 * 4 * 1024;
                int i = 0;

                while (i < length) {
                    //tmpBuffer = ByteBuffer.allocate(len);
                    //channelKeyFile.read(tmpBuffer);
                    keyFile.read(bytes);
                    //bytes = tmpBuffer.array();
                    i += 3 * 4 * 1024;
                    int j = 0;
                    while (j < len) {
                        long tmpKey = 0;
                        for (int k = 0; k < 8; k++) {
                            tmpKey <<= 8;
                            tmpKey |= (bytes[j + k] & 0xff);
                        }
                        int posInt = 0;
                        for (int k = 8; k < 12; k++) {
                            posInt <<= 8;
                            posInt |= (bytes[j + k] & 0xff);
                        }
                        if (tmpKey == 0 && posInt == 0) break;
                        qsortStore.put(tmpKey, posInt);
                        j += 12;
                    }
                }
                qsortStore.sort();
            } catch (Exception e) {
	            e.printStackTrace();
            }
            readyForRange = true;
            System.out.println("readyForRangeCost: " + (System.currentTimeMillis() - start));
        }
    }
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		if (!readyForWrite) {
			readyForWrite();
		}
		if (key[0] == (byte)48 && key[1] == (byte)48 && key[2] == (byte)49 && key[3] == (byte)32) {
			Util.printBytes(key);
			Util.printBytes(value);
		}
		try {
		    long tmpKey = 0;
			for (int i = 0; i < 8; i++) {
				tmpKey <<= 8;
				tmpKey |= (key[i] & 0xff);
			}
			int fileIndex = (int)(tmpKey % FILENUM);
			if (fileIndex < 0) {
				fileIndex += FILENUM;
			}
			long pos;
			synchronized (valueFiles[fileIndex]) {
//                if (countValueFile % MAPSIZE == 0) {
//                    buffValueFile = valueFile.getChannel().map(FileChannel.MapMode.READ_WRITE, countValueFile, MAPSIZE);
//                }
//				pos = countValueFile;
				//buffValueFile = valueFile.getChannel().map(FileChannel.MapMode.READ_WRITE, pos, 4096);
//				buffValueFile.put(value);
//				countValueFile += 4 * 1024;
//                if (countValueFile % MAPSIZE == 0) {
//                    buffValueFile.force();
//                }
                pos = valueFiles[fileIndex].length();
                valueFiles[fileIndex].write(value);
			}
			byte[] newKey = new byte[12];
			for (int i = 0; i < 8; i++) {
				newKey[i] = key[i];
			}
			int posInt = (int)(pos>>>12);
            for (int i = 0; i < 4; i++) {
                int offset = 32 - (i + 1) * 8;
                newKey[8 + i] = (byte) ((posInt >>> offset) & 0xff);
            }
			synchronized (keyFile) {
				buffKeyFile.put(newKey);
				// countKeyFile += 12;
				// if (countKeyFile % (3l * 1024) == 0) {
				// 	buffKeyFile.force();
				// }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		if (!readyForRead) {
			readyForRead();
		}
		long tmpKey = 0;
		for (int i = 0; i < 8; i++) {
			tmpKey <<= 8;
			tmpKey |= (key[i] & 0xff);
		}
		//System.out.println(tmpKey);
		//System.out.println(position);
		long posInt = position.getOrDefault(tmpKey, -1);
		if (posInt == -1) {
			//for (int k = 0; k < 8; k++) {
				//System.out.print(key[k]);
			//}
			throw new EngineException(RetCodeEnum.NOT_FOUND, "");
		}
        long tmpPos =  posInt;
        tmpPos <<= 12;
		//System.out.println(tmpPos);
		int fileIndex = (int)(tmpKey % FILENUM);
		if (fileIndex < 0) {
			fileIndex += FILENUM;
		}
		byte[] value = new byte[4 * 1024];
        //System.out.println(tmpPos >>> 32);
		//System.out.println(valueFileIndex);
        //System.out.println(pos);
		try {
			synchronized (valueFiles[fileIndex]) {
                valueFiles[fileIndex].seek(tmpPos);
                valueFiles[fileIndex].read(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(2);
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
		long start = System.currentTimeMillis();
        if (!readyForRange) {
            readyForRange();
        }
        long l;
        long r;
        if (lower == null) {
            l = 0;
        } else {
            l = Util.bytesToLong(lower);
        }
        if (upper == null) {
            r = -1l;
        } else {
            r = Util.bytesToLong(upper);
        }
        synchronized (this) {
            if (threadId == -1) {
                qsortStore.pool = Executors.newFixedThreadPool(16);
                threadId = Thread.currentThread().getId();
            }
        }
        if (threadId == Thread.currentThread().getId()) {
            for (int i = 0; i < qsortStore.size; i++) {
                qsortStore.locks[i] = 0;
            }
            qsortStore.readAll();
            qsortStore.rangeWithOutReadFirst(l, r, visitor);
        } else {
            qsortStore.rangeWithOutRead(l, r, visitor);
        }

//        if (threadId == Thread.currentThread().getId()) {
//            qsortStore.pool.shutdown();
//        }
		System.out.println(Thread.currentThread().getId() + " RangeCost: " + (System.currentTimeMillis() - start));
	}
	
	@Override
	public void close() {
//	    try {
//            for (RandomAccessFile f : valueFiles) {
//                System.out.println(f.length());
//            }
//        } catch (Exception e) {
//
//        }
        if (qsortStore != null) {
            qsortStore.pool.shutdown();
            System.out.println("pool.shutdown");
        }
		System.out.println("Cost: " + (System.currentTimeMillis() - this.start));
        System.out.println("------");
		System.out.println("close");
        System.out.println("------");
	}

}
