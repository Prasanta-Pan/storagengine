package org.pp.storagengine.api.imp;

import java.io.RandomAccessFile;

import org.pp.storagengine.api.CacheEntry;

class RandFileCacheEntry implements CacheEntry {
    /**
     * Random access file
     */
	private RandomAccessFile rf;
	
	/**
	 * only constructor
	 * @param rf
	 */
	RandFileCacheEntry (RandomAccessFile rf) {
		this.rf = rf;
	}

	@Override
	public int entrySize() {
		// TODO Auto-generated method stub
		return 1;
	}

	public RandomAccessFile getRf() {
		return rf;
	}
	
}
