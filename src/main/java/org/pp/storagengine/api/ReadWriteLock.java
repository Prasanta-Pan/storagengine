package org.pp.storagengine.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pp.storagengine.api.KeyLockerNew.LockKey;

public abstract class ReadWriteLock {
	/**
	 * boolean read constant
	 */
	static final boolean READ  = true;
	/**
	 * boolean write constant
	 */
	static final boolean WRITE = false;
	
	/**
	 * Backing hash map to support concurrency
	 */
	static final ConcurrentMap<Object, ReadWriteLock> map = new ConcurrentHashMap<>(128);
	/**
	 * Lock key
	 */
	protected LockKey lk;
	/**
	 * Lock type
	 */
	protected boolean lType;
	/**
	 * Number of readers currently in progress
	 */
	protected int rCount;
	/**
	 * If wait enabled
	 */
	protected boolean wait = true;
	/**
	 * Writer in progress indicator
	 */
	protected boolean wLock = false;
	/**
	 * Indicate if duplicate request
	 */
	protected boolean duplicate = false;
	
	/**
	 * Unlock key
	 */
	public abstract void unlock();
	
	/**
	 * 
	 * @param lt
	 * @param wLock
	 */
	protected ReadWriteLock init(boolean lt, LockKey lk) {
		this.lType = lt;
		this.lk = lk;
		this.rCount++;
		return this;
	}
	
	@Override
	protected ReadWriteLock clone() {
		ReadWriteLockImp c = new ReadWriteLockImp();
		c.duplicate = true;
		c.lk = lk;
		c.lType = lType;
		c.wait = wait;
		c.wLock = wLock;
		c.rCount = rCount;
		return c;
	}
	
	/**
	 * Read lock against the given key
	 * 
	 * @param key
	 * @return
	 */
	public static final ReadWriteLock readLock(Object key) throws InterruptedException {
		 // Get current thread ID		 
		long tIdLocal = Thread.currentThread().getId(); 
		// Create Lock Key		 
		LockKey nk = new LockKey(key, tIdLocal) ;
		// Create a new Key Locker instance
		ReadWriteLock kl = new ReadWriteLockImp().init(READ, nk);
		for (;;) {
			// Try lock the key
			ReadWriteLock tmp = map.putIfAbsent(key, kl);
			// if same thread trying to lock the same key once again
			if (tmp != null && map.containsKey(nk)) {
				System.out.println(Thread.currentThread().getName() + " already hold the lock");
				return tmp.clone();
			}
			// Lock the key along with thread id
			map.put(nk, tmp == null ? kl : tmp);
			// If lock was previously obtained than return
			if (tmp == null) {
				System.out.println(Thread.currentThread().getName() + " got the lock");
				return kl;
			}
			// When some one locked the key already
			synchronized (tmp) {
				// If wait was enable
				if (tmp.wait) {
					// If writer was not in progress
					if (!tmp.wLock) {
						tmp.rCount++;
						return tmp;
					}
					System.out.println(Thread.currentThread().getName() + " writer in progress, going to sleep");
					// else wait till writer finished
					tmp.wait();
				}
			}
			// just remove the mapping before trying again
			map.remove(nk);
			System.out.println(Thread.currentThread().getName() + " trying again");
		}    	
	}

	/**
	 * Write lock against the key
	 * 
	 * @param key
	 * @return
	 */
	public static final ReadWriteLock writeLock(Object key) throws InterruptedException {
		// Get current thread ID
		long tIdLocal = Thread.currentThread().getId();
		// Create Lock Key
		LockKey nk = new LockKey(key, tIdLocal);
		// Create a new Key Locker instance
		ReadWriteLock kl = new ReadWriteLockImp().init(WRITE, nk);

		for (;;) {
			// Try lock the key
			ReadWriteLock tmp = map.putIfAbsent(key, kl);
			// if same thread trying to lock the same key once again
			if (tmp != null && map.containsKey(nk)) {
				System.out.println(Thread.currentThread().getName() + " already hold the lock");
				return tmp;
			}
			// Lock the key along with thread id
			map.put(nk, tmp == null ? kl : tmp);
			// If lock was previously obtained than return
			if (tmp == null) {
				System.out.println(Thread.currentThread().getName() + " got the lock");
				return kl;
			}
			// When some one locked the key already
			synchronized (tmp) {
				// If wait was enable
				if (tmp.wait) {
					// Signal everyone that writing in progress
					tmp.wLock = true;
					// Wait if read count is greater than zero
					if (tmp.rCount > 0) {
						System.out.println(Thread.currentThread().getName() + " going to sleep");
						tmp.wait();
					}
					// wait is over return with lock
					return tmp;
				}
			}
			// just remove the mapping before trying again
			map.remove(nk);
			System.out.println(Thread.currentThread().getName() + " trying again");
		}    	
	}	
}
