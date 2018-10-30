package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

public class EngineRace extends AbstractEngine {
	Store store;
	@Override
	public void open(String path) throws EngineException {
		try {
			Store.start(path);
		}
		catch (Exception e) {
		}
		store = Store.store;
		try {
		}
		catch (Exception e) {
		}
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		try {
			store.write(key, value);
		}
		catch (Exception e) {
		}
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		byte[] value = null;
		try {
			value = store.read(key);
		}
		catch (Exception e) {
		}
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
	}
	
	@Override
	public void close() {
	}

}
