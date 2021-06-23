package org.pp.storagengine.api.imp;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * The purpose of key locker class is to provide 
 * synchronized access to a particular key to threads
 * @author prasantsmac
 *
 * @param <T>
 */
public class KeyLocker<T> {
	/**
	 * Backing concurrent map
	 */
    private ConcurrentMap<T, Monitor> map = null;
    
    /**
     * Default constructor
     */
	public KeyLocker() {
		map = new ConcurrentHashMap<>();
	}
	
	/**
	 * Constructor with level of concurrency
	 * @param concurrencyLevel
	 */
	public KeyLocker(int concurrencyLevel) {
		map = new ConcurrentHashMap<>(concurrencyLevel);
	}
   
   /**
    * Lock a key or wait if other thread locked the same key
    * @param key
    */
   public void lock(T key) throws InterruptedException {
		Monitor eMon = null;
		Monitor newMon = new Monitor();
		for (;;) {
			eMon = map.putIfAbsent(key, newMon);
			/**
			 * Return immediately if succeed  
			 */
			if (eMon == null)
				break;
			/**
			 * Some thread already locked the key.
			 * Check if wait is possible or not.			 * 
			 */
			synchronized (eMon) {
				/**
				 * Ensure wait was enabled in order avoid
				 * forever lock
				 */
				if (eMon.wait) {
					eMon.wait();
				}
			}
		}
    }
    
   /**
    * Unlock the the same key used to lock previously
    * @param key
    */
    public void unlock(T key) {
    	Monitor monitor = map.remove(key);
    	if (monitor != null) {
    	    synchronized (monitor) {
    	    	/**
    	    	 * Ensure no thread ever sleep 
    	    	 * on the same monitor
    	    	 */
    	       monitor.wait = false;
    		   monitor.notifyAll();
			}
    	}
    } 
    
    /**
     * 
     * @author prasantsmac
     *
     */
    private static final class Monitor { 
    	/**
    	 * Indicate whether waiting is required or not
    	 */
    	private boolean wait = true;     	
    	
    }
    
    // Test key locker implementation
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final KeyLocker<Integer> kLocker = new KeyLocker<>();
		final AtomicInteger check = new AtomicInteger(0);
		for(int i=0;i<6;i++) {
			new Thread(new Runnable() {
				@SuppressWarnings("static-access")
				@Override
				public void run() {
					// TODO Auto-generated method stub
					ThreadLocalRandom rand = ThreadLocalRandom.current();
					String tName = Thread.currentThread().getName();
					Thread t = Thread.currentThread();
					int count = 0;
					for(;;) {
						count++;
						int val = rand.nextInt(5,12);				
						try {
							kLocker.lock(val);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
							throw new RuntimeException(e1);
						}
						try {
							System.out.println(tName + " with key " + val + " running");
							t.sleep(rand.nextInt(5, 10) * 10);
						}
						catch (Exception e) { }
						finally {
							if(check.compareAndSet(count - 1,count))
								System.out.println("######################################################");
							kLocker.unlock(val);							
						}
					}					
				}
			}).start();
		}
	}
}
