package org.pp.storagengine.api.imp;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.pp.storagengine.api.CacheEntry;
/**
 * LRU cache implementation which serve dual purpose.
 * Cache can be maintained either by the number of entries or size
 * @author prasantsmac
 *
 * @param <K>
 * @param <V>
 */
public class LRUCache <K extends CacheEntry, V extends CacheEntry> {
	/**
	 * Head & Tail pointer
	 */
	private LRUNode<K, V> head, tail;
	/**
	 * Backing concurrent hash map
	 */
	private final ConcurrentMap<K, LRUNode<K, V>> map;
	/**
	 * Lock to be used
	 */
	private final Lock lock = new ReentrantLock(true);
	/**
	 * current size of the cache
	 */
	private volatile long size ;
	/**
	 * max cache size
	 */
	private long maxSz;
	
   /**
    * Only LRU Cache constructor
    * @param maxCapa - Initial capacity of of backing hash map
    * @param maxSize - Maximum cache size
    */
	public LRUCache(int maxCapa, long maxSize) {
		if (maxCapa <= 0)
			throw new RuntimeException("Capacity can not be zero or negative");
		map = new ConcurrentHashMap<>(maxCapa);
		maxSz = maxSize;
	}
   
    /**
     * Return apprx size
     * @return
     */
	public long size() {
		long sz = size;
		return sz;
	}
	/**
	 * put an entry
	 * @param key
	 * @param value
	 * @return
	 */
	public V put(K key, V value) {
		return put(key, value, false);
	}
	/**
	 * Put only if absent
	 * @param key
	 * @param value
	 * @return
	 */
	public V putIfAbsent(K key, V value) {
		return put(key, value, true);
	}
	/**
	 * Get is non blocking lock free
	 * @param key
	 * @return
	 */
	public V get(K key) {
		LRUNode<K, V> lruNode = map.get(key);
		return lruNode != null ? lruNode.value : null;
	}
	/**
	 * get with moving position
	 * @param key
	 * @param mv
	 * @return
	 */
	public V get(K key, boolean mv) {
		LRUNode<K, V> lruNode;
		// get lock
		lock.lock();
		try {
			if ((lruNode = map.get(key)) != null) {
				// remove it from its current position
				removeNode(lruNode);
				// move it to front
				tailIt(lruNode);
			}			
		} finally {
			lock.unlock();
		}
		return lruNode != null ? lruNode.value : null;
	}
	/**
	 * Remove mapping
	 * @param key
	 * @return
	 */
	public V remove(K key) {
		if (key == null)
			throw new NullPointerException("Key can not be null");
		// obtain lock
		lock.lock();
		try {
			LRUNode<K, V> lrEntry;
			return  (lrEntry = delete(key)) != null ? lrEntry.value : null;
		} finally {
			lock.unlock();
		}
	}
	/**
	 * Remove oldest entry
	 * @return
	 */
	public V removeOldest() {
		lock.lock();
		try {
			// check if any entry available to remove or not.
			if (head == null)
				return null;			
			LRUNode<K, V> lrEntry;
			return (lrEntry = delete(head.key)) != null ? lrEntry.value : null;
		} finally {
			lock.unlock();
		}
	}
	/**
	 * Remove the oldest entry
	 * @return
	 */
	public LRUNode<K, V> removeOldestEntry() {
		lock.lock();
		try {
			// check if any entry available to remove or not.
			if (head == null)
				return null;	
			K k = head != null ? head.key : null;
			return delete(k);
		} finally {
			lock.unlock();
		}
	}
    /**
     * common method put method
     * @param key
     * @param value
     * @param putIfAbsent
     * @return
     */
	private V put(K key, V value, boolean putIfAbsent) {
		if (key == null || value == null)
			throw new NullPointerException("Key or Value can not be null");
		V val = null;
		lock.lock();
		try {
			LRUNode<K, V> eNode = null;
			LRUNode<K, V> lNode = new LRUNode<>(key, value);
			// return if put if absent and entry is present
			if (putIfAbsent && (eNode = map.putIfAbsent(key, lNode)) != null) {
				return eNode.value;
			} 
			// either
			if (!putIfAbsent && (eNode = map.put(key, lNode)) != null) {
				// remove old mapping (if any) and adjust size
				updSize(eNode.key, eNode.value, false);
				removeNode(eNode);
				val = eNode.value;
			}			
			// add new node to tail
			tailIt(lNode); 
			// update size
			updSize(key, value, true); 
			// check if size exceed
			while (maxSz > 0 && size > maxSz && head != null) {
				delete(head.key);
			}
		} finally {
			lock.unlock();
		}
		return val;
	}

	/**
	 * Delete LRU node
	 * 
	 * @param key
	 * @return
	 */
	private LRUNode<K, V> delete(K key) {
		LRUNode<K, V> lruNode;
		// if only node
		if ((lruNode = map.remove(key)) != null) {
			// remove node
			removeNode(lruNode);
			// update size
			updSize(lruNode.key, lruNode.value, false);
		}
		// return node
		return lruNode;
	}

	/**
	 * Update current cache size
	 * 
	 * @param k
	 * @param v
	 * @param up
	 */
	private void updSize(K k, V v, boolean up) {
		// key size
		int ks = k.entrySize();
		// value size
		int vs = v.entrySize();;
		
		if (!up) {
			size -= ks;
			size -= vs;
		} else {
			size += ks;
			size += vs;
		}
	}
    /**
     * Remove Node
     * @param lruNode
     */
	private void removeNode(LRUNode<K, V> lruNode) {
		LRUNode<K, V> pNode = lruNode.prev;
		LRUNode<K, V> nNode = lruNode.next;
		// Removing in middle
		if (pNode != null && nNode != null) { 
			pNode.next = nNode;
			nNode.prev = pNode;
		}
		// Removing tail
		else if (pNode != null) {
			tail = pNode;
			tail.next = null;
		} 
		// Removing head
		else if (nNode != null) { 
			head = head.next;
			head.prev = null;
		} 
		// Only Node
		else
			head = tail = null; 
	}
    /**
     * Append at tail
     * @param lruNode
     */
	private void tailIt(LRUNode<K, V> lruNode) {
		// first node
		if (tail == null) {
			head = tail = lruNode;
			return;
		}
		// tail it
		lruNode.prev = tail;
		tail = tail.next = lruNode;
	}
	
    /**
     * 
     * @author prasantsmac
     *
     * @param <K>
     * @param <V>
     */
	static final class LRUNode<K, V> {
		private K key;
		private V value;
		private LRUNode<K, V> prev, next;

		private LRUNode() {
		}

		private LRUNode(K key, V value) {
			this.key = key;
			this.value = value;
		}
	}

	public static void main(String[] args) {
		// sizeTest();
		// nonSize();
		// LRUCache<MyKey, MyValue> lru = new LRUCache<>(128 * 1024, true,40000 * 200);
		// doAction(lru);
		doItParralel();
	}

	public static void nonSize() {
		// No Size
		LRUCache<MyKey, MyValue> lruCache = new LRUCache<>(32, 32 * 2);
		doAction(lruCache, null, "put", 5, 10, 15, 17, 21, 35);
		doAction(lruCache, null, "putIfAbsent", 35, 17, 76);
		doAction(lruCache, null, "put", 5, 17, 76, 11);
		doAction(lruCache, null, "remove", 10, 11, 35);
		doAction(lruCache, null, "get", 17, 21, 76);
		doAction(lruCache, null, "Remove Oldest", 3);
	}

	public static void sizeTest() {
		// Using size
		LRUCache<MyKey, MyValue> lruCache = new LRUCache<>(64, 8 * 200);
		int[] valArray = new int[] { 40, 100, 150, 300, 500 };
		doAction(lruCache, valArray, "put", 5, 10, 15, 17, 21, 35, 37, 45, 100);
		doAction(lruCache, valArray, "putIfAbsent", 35, 17, 76);
		doAction(lruCache, valArray, "put", 5, 17, 76, 11);
		doAction(lruCache, valArray, "remove", 10, 11, 35);
		doAction(lruCache, valArray, "get", 17, 21, 76);
		doAction(lruCache, valArray, "Remove Oldest", 3);
	}

	public static void doItParralel() {
		final LRUCache<MyKey, MyValue> lru = new LRUCache<>(128 * 1024, 40000 * 200);
		for (int i = 0; i < 4; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					// TODO Auto-generated method stub
					doAction(lru);
				}
			}).start();
		}
	}

	@SuppressWarnings("rawtypes")
	private static void printList(LRUNode head) {
		StringBuilder sBldr = new StringBuilder("[");
		for (LRUNode tmp = head; tmp != null; tmp = tmp.next)
			sBldr.append("(" + tmp.key + "," + tmp.value + "),");
		String str = sBldr.substring(0, sBldr.length() - 1) + "]";
		System.out.println(str);
	}

	private static void doAction(LRUCache<MyKey, MyValue> lru, int[] size, String action, int... keys) {
		StringBuilder sBldr = new StringBuilder();
		for (int i = 0; i < keys.length; i++) {
			MyKey key = new MyKey(keys[i]);
			switch (action.trim()) {
			case "put":
				lru.put(key, getVal(size));
				sBldr.append(key + ",");
				break;
			case "putIfAbsent":
				lru.putIfAbsent(key, getVal(size));
				sBldr.append(key + ",");
				break;
			case "remove":
				lru.remove(key);
				sBldr.append(key + ",");
				break;
			case "get":
				sBldr.append("(" + key + "," + lru.get(key) + "),");
				break;
			default:
				for (int j = 0; j < keys[i]; j++)
					lru.removeOldest();
				System.out.println("Remove oldest..." + keys[i]);
				printList(lru.head);
				System.out.println("Size.." + lru.size());
				return;
			}
		}
		String msg = action + "...";
		System.out.println(msg + sBldr.substring(0, sBldr.length() - 1));
		printList(lru.head);
		System.out.println("Size.." + lru.size());
	}

	private static void doAction(LRUCache<MyKey, MyValue> lru) {
		String[] actions = new String[] { "put", "putIfAbsent", "get", "get" };
		int[] size = new int[] { 40, 100, 150, 300, 500 };
		ThreadLocalRandom rand = ThreadLocalRandom.current();

		String action = null;
		long maxTime = 15 * 1000;
		long time = System.currentTimeMillis();
		MyKey key = null;
		int count = 0;
		int put = 0;
		int putIfAbsent = 0;
		int get = 0;
		while (System.currentTimeMillis() - time < maxTime) {
			action = actions[rand.nextInt(0, 4)];
			key = new MyKey(rand.nextInt(0, 50000));
			switch (action) {
			case "put":
				lru.put(key, getVal(size));
				put++;
				break;
			case "putIfAbsent":
				lru.putIfAbsent(key, getVal(size));
				putIfAbsent++;
				break;
			default:
				lru.get(key);
				get++;
			}
			count++;
		}
		System.out.println(Thread.currentThread().getName() + " Number of actions..." + count);
		System.out.println(Thread.currentThread().getName() + " Number of gets..." + get);
		System.out.println(Thread.currentThread().getName() + " Number of puts..." + put);
		System.out.println(Thread.currentThread().getName() + " Number of putIfAbsent..." + putIfAbsent);
	}

	private static MyValue getVal(int[] size) {
		ThreadLocalRandom rand = ThreadLocalRandom.current();
		if (size == null)
			return new MyValue(rand.nextInt(0, 1000));
		byte[] data = new byte[size[rand.nextInt(0, size.length)]];
		rand.nextBytes(data);
		MyValue val = new MyValue(data);
		return val;
	}

	private static final class MyKey implements CacheEntry {
		private int key;

		private MyKey(int key) {
			this.key = key;
		}

		@Override
		public int entrySize() {
			// TODO Auto-generated method stub
			return 4;
		}

		@Override
		public String toString() {
			return "" + key;
		}

		@Override
		public int hashCode() {
			return key;
		}

		@Override
		public boolean equals(Object othr) {
			return ((MyKey) othr).key != key ? false : true;
		}
	}

	private static final class MyValue implements CacheEntry {
		private Object value = null;

		private MyValue(Object val) {
			value = val;
		}

		@Override
		public int entrySize() {
			// TODO Auto-generated method stub
			return ((byte[]) value).length;
		}

		@Override
		public String toString() {
			if (value instanceof Integer)
				return value.toString();
			return ((byte[]) value).length + "B";
		}
	}
}
