package org.pp.storagengine.api.imp;

import org.pp.storagengine.api.CacheEntry;

class IntCacheEntry implements CacheEntry {
	/**
	 *  INT value
	 */
    private int val;
    
    /**
     * 
     * @param val
     */
    IntCacheEntry (int val) {
    	this.val = val;
    }
	
	@Override
	public int entrySize() {
		// TODO Auto-generated method stub
		return 1;
	}
	
	@Override
	public int hashCode() {
		return val;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof IntCacheEntry) {
			IntCacheEntry oth = (IntCacheEntry) o;
			if (oth.val == val)
				return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return "" + val;
	}
	//
	int getVal() {
		return val;
	}

}
