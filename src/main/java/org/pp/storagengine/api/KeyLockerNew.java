package org.pp.storagengine.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract Key locker
 * @author prasantsmac
 *
 */
public abstract class KeyLockerNew {
	/**
	 * Thread based Lock key
	 * @author prasantsmac
	 *
	 */
	static final class LockKey {
		/**
		 * User provided key 
		 */
		Object key;
		/**
		 * Thread ID
		 */
		long tId;
		
		/**
		 * New LockKey with thread ID
		 * @param k
		 * @param tId
		 */
		LockKey(Object k, long tId) {
			this.key = k;
			this.tId = tId;
		}
		
		@Override
		public int hashCode() {
			return 31 * key.hashCode() + (int) tId;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof LockKey) {
				LockKey oth = (LockKey) o;
				if (key.equals(oth.key) && oth.tId == tId)
					return true;
			}
			return false;
		}
	}	
	/**
	 * Backing hash map to support concurrency
	 */
	static final ConcurrentMap<Object, KeyLockerNew> map = new ConcurrentHashMap<>(128);
	/**
	 * Lock key
	 */
	protected LockKey lk;
	/**
	 * If wait enabled
	 */
	protected boolean wait = true;	
	/**
	 * Unlock key
	 */
	public abstract void unlock();
	
	/**
	 * 
	 * @param lt
	 * @param wLock
	 */
	protected KeyLockerNew init(LockKey lk) {
		this.lk = lk;		
		return this;
	}
	
	/**
	 * Basic Key locker. 
	 * Only one thread can proceed to lock a particular key
	 * @param key
	 * @return
	 * @throws InterruptedException 
	 */
	public static final KeyLockerNew lock(Object key) throws InterruptedException {
		// Get current thread ID
		long tIdLocal = Thread.currentThread().getId();
		// Create Lock Key
		LockKey nk = new LockKey(key, tIdLocal);
		// Create a new Key Locker instance
		KeyLockerNew kl = new KeyLockerImp().init(nk);

		for (;;) {
			// Try lock the key
			KeyLockerNew tmp = map.putIfAbsent(key, kl);
			// if same thread trying to lock the same key once again
			if (tmp != null && map.containsKey(nk))
				return tmp;
			// Lock the key along with thread id
			map.put(nk, tmp == null ? kl : tmp);
			// If lock was already obtained
			if (tmp == null)
				return kl;
			// If all the above option fail than wait
			synchronized (tmp) {
				if (tmp.wait)
					tmp.wait();
			}
			// Remove the mapping before try again
			map.remove(nk);
		}    	
	}
}
