package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongLongHashMap;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class EngineRace extends AbstractEngine {
	final static long MAPSIZE = 16 * 4 * 1024 * 1024l;
	//final static long FILESIZE =  4 * 1024 * 1024 * 1024;
    final static long FILENUM =  256;
	String path;
	//DiyHashMap position;
	LongLongHashMap position;
	RandomAccessFile keyFile;
	MappedByteBuffer buffKeyFile;
	//MappedByteBuffer buffValueFile;
	boolean readyForRead = false;
	boolean readyForWrite = false;
	//FileChannel channel;
	long countKeyFile = 0l;
	//long countValueFile = 0l;
	List<RandomAccessFile> valueFiles;
	@Override
	public void open(String path) throws EngineException {
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
			try {
				keyFile = new RandomAccessFile(this.path + "keyFile", "r");
				valueFiles = new ArrayList<RandomAccessFile>();
				for (int i = 0; i < FILENUM; i++) {
				    RandomAccessFile f = new RandomAccessFile(this.path + "valueFile" + i, "rw");
				    f.seek(f.length());
				    valueFiles.add(f);
                }
				//valueFile = new RandomAccessFile(this.path + "valueFile.data", "r");
				//position = new DiyHashMap(64000000);
				//position = new DiyHashMap(3);
				position = new LongLongHashMap(64000000, 0.99);
                //position = new LongLongHashMap();
				int length = (int) keyFile.length();
				//System.out.println(length);
				byte[] bytes = new byte[(int)MAPSIZE];
				int len;
				int i = 0;
				while (i < length) {
					if (length - i >= MAPSIZE - 1) {
						len = (int)MAPSIZE;
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
						if (tmpKey == 0 && tmpPos == 0) {
							break;
						}
						position.put(tmpKey, tmpPos);
						j += 16;
					}
				}
				readyForRead = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("------");
			System.out.println("readyForRead");
			System.out.println("------");
		}
	}

	synchronized public void readyForWrite(){
		if (!readyForWrite) {
			try {
				keyFile = new RandomAccessFile(this.path + "keyFile", "rw");
				countKeyFile = keyFile.length();
				//channel = keyFile.getChannel();
                valueFiles = new ArrayList<RandomAccessFile>();
				for (int i = 0; i < FILENUM; i++) {
				    RandomAccessFile f = new RandomAccessFile(this.path + "valueFile" + i, "rw");
				    f.seek(f.length());
				    valueFiles.add(f);
                }
				readyForWrite = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("------");
			System.out.println("readyForWrite");
			System.out.println("------");
		}
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		if (!readyForWrite) {
			readyForWrite();
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
			RandomAccessFile f = valueFiles.get(fileIndex);
			synchronized (f) {
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
                pos = f.length();
				f.write(value);
			}
			byte[] newKey = new byte[16];
			for (int i = 0; i < 8; i++) {
				newKey[i] = key[i];
			}
            for (int i = 0; i < 8; i++) {
                int offset = 64 - (i + 1) * 8;
                newKey[8 + i] = (byte) ((pos >>> offset) & 0xff);
            }
			synchronized (keyFile) {
				if (countKeyFile % MAPSIZE == 0) {
					buffKeyFile = keyFile.getChannel().map(FileChannel.MapMode.READ_WRITE, countKeyFile, MAPSIZE);
				}
				//keyFile.write(newKey);
				buffKeyFile.put(newKey);
				countKeyFile += 16;
				if (countKeyFile % MAPSIZE == 0) {
					buffKeyFile.force();
				}
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
		long tmpPos = position.getOrDefault(tmpKey, -1l);
		if (tmpPos == -1l) {
			//for (int k = 0; k < 8; k++) {
				//System.out.print(key[k]);
			//}
			throw new EngineException(RetCodeEnum.NOT_FOUND, "");
		}
		//System.out.println(tmpPos);
		int fileIndex = (int)(tmpKey % FILENUM);
		if (fileIndex < 0) {
			fileIndex += FILENUM;
		}
		RandomAccessFile f = valueFiles.get(fileIndex);
		byte[] value = new byte[4 * 1024];
        //System.out.println(tmpPos >>> 32);
		//System.out.println(valueFileIndex);
        //System.out.println(pos);
		try {
			synchronized (f) {
				f.seek(tmpPos);
				f.read(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(2);
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
	}
	
	@Override
	public void close() {
	    if (readyForWrite) {
            try {
                //buff.force();
//                keyFile.close();
//                valueFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("------");
		System.out.println("close");
        System.out.println("------");
	}

}
