package org.pp.storagengine.api;

public interface KVEngine {
	/**
	 * Ensure key size should not be too long to avoid any performance problem
	 */
	public static final int MAX_KEY_SIZE = 256;
	/**
	 * Call sync manually, only write ahead log will be Synced if log is enabled.
	 * either caller will be blocked until internal cache is safely persisted to
	 * underlying storage device.This call might be expensive. How long sync will
	 * take to return depends upon the current write cache size. One should adjust
	 * the write cache size accordingly.
	 **/
	public void sync() throws Exception;
	/**
	 * Get runtime statistics of a running instance of KVEngine. The same been has
	 * been exposed as JMX MBean. one can retrieve live statistics by connecting any
	 * JMX agent (e.g. JVisualVM/JConsole) to the running instance.
	 **/
	public StatisticsMXBean getStatistics();
	/**
	 * find a value associated with this key, return null if no value associated
	 * with this key.
	 **/
	public KVEntry get(byte[] key) throws Exception;

	/**
	 * Delete a mapping associated with the provided key. Note that value will be
	 * null on returned Entry. Return entry is an instance of KVEntry.
	 **/
	public KVEntry delete(byte[] key) throws Exception;
	/**
	 * Create a new (or update any existing) mapping and return the entry. Return
	 * entry is an instance of KVEntry.
	 */
	public KVEntry put(byte[] key, byte[] value) throws Exception;
	/**
	 *  Get the iterator to travers first to end key
	 * @return
	 * @throws Exception
	 */
	public KVIterator iterator() throws Exception;
	/**
	 * Get a iterator of given iterator. 'true' for reverse otherwise natural order
	 * @param rev
	 * @return
	 * @throws Exception
	 */
	public KVIterator iterator(boolean rev) throws Exception;
	/**
	 * Get the iterator which start with spesified key and travers to the end
	 * @param start
	 * @return
	 * @throws Exception
	 */
	public KVIterator iterator(byte[] start) throws Exception;
	/**
	 * Get the iterator which start with spesified key and order
	 * @param start
	 * @return
	 * @throws Exception
	 */
	public KVIterator iterator(byte[] start, boolean rev) throws Exception;
	/**
	 *  Get the range iterator
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 */
	public KVIterator iterator(byte[] start,byte[] end) throws Exception; 
	/**
	 * Get the range iterator with given order
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 */
	public KVIterator iterator(byte[] start,byte[] end, boolean rev) throws Exception; 
	
	// key related API
	/**
	 * Get the first key
	 * @return
	 * @throws Exception
	 */
	public KVEntry firstEntry() throws Exception;
	/**
	 *  Return the last key
	 * @return
	 * @throws Exception
	 */
	public KVEntry lastEntry() throws Exception;
	/**
	 * Return next key of a given key if exist
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public KVEntry nextEntry(byte[] key) throws Exception;
	/**
	 * Return previous key if exist of a given key
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public KVEntry prevEntry(byte[] key) throws Exception;
    /**
	 * Close the engine
	 * @throws Exception
	 */
	public void close() throws Exception;
	
}
