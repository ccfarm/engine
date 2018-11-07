package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

public class EngineRace extends AbstractEngine {
	@Override
	public void open(String path) throws EngineException {
		try {
			Store.store.start(path);
		}
		catch (Exception e) {
			System.out.println(e);
		}

	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		try {
            Store.store.write(key, value);
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		byte[] value = null;
		try {
			value = Store.store.read(key);
		}
		catch (EngineException e) {
			throw e;
		}
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
	}
	
	@Override
	public void close() {
        System.out.println("------");
		System.out.println("close");
        System.out.println("------");
        Store.store.end();
	}

}
