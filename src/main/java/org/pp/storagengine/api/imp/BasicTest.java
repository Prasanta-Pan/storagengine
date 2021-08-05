package org.pp.storagengine.api.imp;
import static org.pp.storagengine.api.imp.Util.GB;
import static org.pp.storagengine.api.imp.Util.MB;
import static org.pp.storagengine.api.imp.Util.KB;

import java.io.File;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;
import org.pp.storagengine.api.KVIterator;
import org.pp.storagengine.api.StatisticsMXBean;

public class BasicTest {
	// Maximum database size to test
	private static long dbSize = 256 * MB;
	// Database root directory
	private static String ROOT_DIR = null;
	// Tree map to verify entries
	private static NavigableMap<byte[], byte[]> map = null;
	// For digest calculation
	private static MessageDigest md = null;
	// For random number generations
	private static ThreadLocalRandom rand = null;
	// Storage engine
	private static KVEngine db = null;
	// Micro seconds
	private static int mSec = 1000 * 1000;
	// Minimum LOB size
	private static final int minLobSz = 4 * 1024 / 2;

	// Byte array comparator
	private static Comparator<byte[]> myComp = new Comparator<byte[]>() {
		@Override
		public int compare(byte[] left, byte[] right) {
			for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
				int a = (left[i] & 0xff);
				int b = (right[j] & 0xff);
				if (a != b) {
					return a - b;
				}
			}
			return left.length - right.length;
		}
	};

	// Value bytes
	private static final int[] vbytes = new int[] 
		{ 1, 3, 4, 8, 1, 3, 4, 8, 12, 16, 20, 30, 40, 80, 75, 100, 110, 140,
		  150, 210, 240, 300, 320, 350, 511, 575, 590, 600, 630, 710, 810, 819, 
		  890, 901, 1100, 1200, 1350, 3001, 3227, 5 * 1024, 20 * 1024, 32 * 1024, 
		  40 * 1024
		};

	// Random key generator
	private static byte[] genKey() {
		//
		byte[] key = new byte[rand.nextInt(1, 35)];
		for (int i = 0; i < key.length; i++)
			key[i] = (byte) rand.nextInt(0, 20);
		return key;
	}

	// Get a random lower mid key
	private static byte[] getLowerMidKey() {
		byte[] key = new byte[rand.nextInt(1, 35)];
		for (int i = 0; i < key.length; i++)
			key[i] = (byte) rand.nextInt(3, 10);
		return key;
	}

	// get a random higher mid key
	private static byte[] getUpperMidKey() {
		byte[] key = new byte[rand.nextInt(1, 35)];
		for (int i = 0; i < key.length; i++)
			key[i] = (byte) rand.nextInt(13, 20);
		return key;
	}

	// Verify first key in the database
	private static void firstKey() throws Exception {
		System.out.print("\nValidating first key.....");
		byte[] mKey = null;
		try {
			mKey = map.firstKey();
		} catch (NoSuchElementException e) {
		}
		KVEntry entry = db.firstEntry();
		verifyKey(mKey, entry != null ? entry.getKey() : null);
	}

	// Verify last key in the database
	private static void lastKey() throws Exception {
		System.out.print("\nValidating last key.....");
		byte[] mKey = null;
		try {
			mKey = map.lastKey();
		} catch (NoSuchElementException e) {
		}
		KVEntry entry = db.lastEntry();
		verifyKey(mKey, entry != null ? entry.getKey() : null);
	}

	// Verify next key
	private static void nextKey() throws Exception {
		System.out.print("\nValidating next key.....");
		byte[] key = getLowerMidKey();
		byte[] mKey = map.higherKey(key);
		KVEntry entry = db.nextEntry(key);
		verifyKey(mKey, entry != null ? entry.getKey() : null);
	}
	// verify previous key
	private static void prevKey() throws Exception {
		System.out.print("\nValidating prev key.....");
		byte[] key = getLowerMidKey();
		byte[] mKey = map.lowerKey(key);
		KVEntry entry = db.prevEntry(key);
		verifyKey(mKey, entry != null ? entry.getKey() : null);
	}
	// common key verification
	private static void verifyKey(byte[] mKey, byte[] kvKey) {
		if (mKey != null && kvKey != null) {
			if (!Arrays.equals(mKey, kvKey)) {
				throw new RuntimeException(
						"Keys are not matching! \n (" + Arrays.toString(mKey) + "," + Arrays.toString(kvKey) + ")");
			}
		} 
		else if (kvKey != null || mKey != null) {
			throw new RuntimeException("One of the key is null!");
		}
		System.out.print("OK");
	}

	// Random get
	private static void randomGet() throws Exception {
		System.out.print("\nTesting a random get.....");
		byte[] key = genKey();
		byte[] mVal = map.get(key);
		KVEntry entry = db.get(key);

		if (mVal != null && entry != null)
			verify(key, mVal, entry);
		else if (entry != null || mVal != null)
			throw new RuntimeException("One of the value is null!");
		System.out.print("OK");
		System.out.println("\n");
	}

	// Random iteration
	private static void doItr() throws Exception {
		// Iterate over all records in ascending order...
		readAll(null, null, false, "Iterate over all records in ascending order");
		// Iterate over all records in descending order...
		readAll(null, null, true, "Iterate over all records in descending order");
		// Iterate over start (?) - end ascending order...
		readAll(getUpperMidKey(), null, false, "Iterate over start (?) - end ascending order");
		// Iterate over start (?) - end descending order...
		readAll(null, getUpperMidKey(), true, "Iterate over start (?) - end descending order");
		// Iterate over start - end(?) ascending order...
		readAll(null, getLowerMidKey(), false, "Iterate over start - end(?) ascending order");
		// Iterate over start - end(?) descending order...
		readAll(getLowerMidKey(), null, true, "Iterate over start - end(?) descending order");
		// Iterate over start (?) - end(?) ascending order...
		readAll(getLowerMidKey(), getUpperMidKey(), false, "Iterate over start (?) - end(?) ascending order");
		// Iterate over start (?) - end(?) descending order...
		readAll(getUpperMidKey(), getLowerMidKey(), true, "Iterate over start (?) - end(?) descending order");
	}

	// Iterate over records in random order
	private static void readAll(byte[] from, byte[] to, boolean rev, String msg) throws Exception {
		// If reverse ...
		SortedMap<byte[], byte[]> nMap = rev ? map.descendingMap() : map;
		if (from != null && to != null)
			nMap = nMap.subMap(from, to);
		else if (from != null)
			nMap = nMap.tailMap(from);
		else if (to != null)
			nMap = nMap.headMap(to);
		// Start iterating
		Iterator<Entry<byte[], byte[]>> mItr = nMap.entrySet().iterator();
		KVIterator itr = db.iterator(from, to, rev);
		int tCount = nMap.size();
		long tot = 0;
		int count = 0;
		long time = System.currentTimeMillis();
		StringBuilder sbdlr = new StringBuilder("\r" + msg);
		int maxLen = sbdlr.length() + 5;

		try {
			Entry<byte[], byte[]> lmEntry = null;
			KVEntry kvEntry = null;
			long t = System.nanoTime();
			while (itr.hasNext() && mItr.hasNext()) {
				lmEntry = mItr.next();
				kvEntry = itr.next();
				tot += (System.nanoTime() - t);
				verify(lmEntry.getKey(), lmEntry.getValue(), kvEntry);
				t = System.nanoTime();
				count++;
				if (System.currentTimeMillis() - time > 200) {
					sbdlr.append(".");
					System.out.print(sbdlr.toString());
					if (sbdlr.length() == maxLen) {
						sbdlr.setLength(0);
						sbdlr.append("\r" + msg);
					}
					time = System.currentTimeMillis();
				}
			}
		} finally {
			itr.close();
		}
		// if count doesn't match
		if (count != tCount)
			throw new RuntimeException("Count doesn't match!");
		System.out.print("\r" + msg + ": " + (tot / mSec) + "," + count + "\n");
	}

	// Verify key value pair
	private static void verify(byte[] key, byte[] val, KVEntry kvEntry) throws Exception {
		if (!Arrays.equals(key, kvEntry.getKey()))
			throw new RuntimeException("Key's are not matching!\n (" + Arrays.toString(key) 
			+ "," + Arrays.toString(kvEntry.getKey()) + ")");
		// Recalculate digest
		byte[] dgst = getDigest(kvEntry.getKey(), kvEntry.getValue());
		if (!Arrays.equals(val, dgst))
			throw new RuntimeException("Digest's are not matching!");
	}

	// Put random records
	private static void put() throws Exception {
		byte[] key, value;
		KVEntry entry = null;
		// Write first
		long t = 0, tot = 0, size = dbSize / 2 ;
		int ne = 0, prog = 0, lastProg = 0;
		for (long sz = 0; sz < size; sz += (key.length + value.length), ne++) {
			value = new byte[vbytes[rand.nextInt(0, vbytes.length)]];
			key = genKey();
			rand.nextBytes(value);
			t = System.nanoTime();
			entry = db.put(key, value);
			tot += (System.nanoTime() - t);
			map.put(entry.getKey(), getDigest(entry.getKey(), entry.getValue()));
			prog = (int) ((100.0f * sz) / size);
			if (prog > lastProg) {
				System.out.print("\rPut test in progress..." + prog + "%");
				lastProg = prog;
			}
		}
		System.out.println("\n" + ne + ", Tot time to put in (ms): " + (tot / mSec));
	}

	// Delete random records
	private static void delete() throws Exception {
		long t = 0, tot = 0, size = dbSize / 4 ;
		int i = 0, prog = 0;
		KVEntry entry;
		// delete LOB only to trigger LOB deletion thread
		KVIterator itr = db.iterator(null, null);
		try {
			for (long sz = 0; itr.hasNext() && sz < size;) {
				entry = (KVEntry) itr.next();
				if (entry.size() > minLobSz) {
					t = System.nanoTime();
					db.delete(entry.getKey());
					tot += (System.nanoTime() - t);
					map.remove(entry.getKey());
					i++; sz += entry.size();
					prog = (int) ((100.0f * sz) / size);
					System.out.print("\rDelete in progress..." + prog + "%");
				}
			}
			System.out.println("\n" + i + " Delete in (ms): " + (tot / mSec));
		} finally {
			itr.close();
		}
	}

	// get random records
	private static void get() throws Exception {
		List<byte[]> list = new ArrayList<>(map.keySet());
		Collections.shuffle(list);
		long t = 0, tot = 0, size = dbSize / 2 ;
		int i = 0, prog = 0, lastProg = 0;
		KVEntry entry;
		try {
			for (long sz = 0; sz < size && i < list.size(); sz += entry.size(), i++) {
				t = System.nanoTime();
				entry = db.get(list.get(i));
				tot += (System.nanoTime() - t);
				verify(list.get(i), map.get(list.get(i)), entry);
				prog = (int) ((100.0f * sz) / size);
				if (prog > lastProg) {
					System.out.print("\rGet in progress..." + prog + "%");
					lastProg = prog;
				}
			}
			System.out.println("\n" + i + " get in (ms): " + (tot / mSec));
		} finally {
			list.clear();
		}
	}
	
	private static void printState() {
		StatisticsMXBean mbean = db.getStatistics();
		StringBuilder sbldr = new StringBuilder("################################# Statistics Start #####################################" + "\n");
		sbldr.append("Apprx db size (MB): " + mbean.getApprxSize() / MB + "\n");
		sbldr.append("Data block size (KB): " + mbean.getDataBlockSize() / KB + "\n");
		sbldr.append("B+ tree hieght : " + mbean.getHt() + "\n");
		sbldr.append("Last sync time : " + new Date(mbean.getLastSyncTime()) + "\n");
		sbldr.append("Max block per file : " + mbean.getMaxBlockPerFile() + "\n");
		sbldr.append("Data file size (MB): " + mbean.getMaxDataFileSize() + "\n");
		sbldr.append("Max LOB size (KB): " + mbean.getMaxLobSize() / KB + "\n");
		sbldr.append("Max sync time(ms) : " + mbean.getMaxSyncTime() + "\n");
		// nodes per level
		for (int i = -1 ; i <= mbean.getHt(); i++) {
			sbldr.append("Nodes per level : " + mbean.getNmOfNode(i) + "/" + i + "\n");
		}
		sbldr.append("Number of active records : " + mbean.getNumOfActRecs() + "\n");
		sbldr.append("Number of branch entries : " + mbean.getNumOfBranchEntry() + "\n");
		sbldr.append("Number of data files : " + mbean.getNumOfDataFiles() + "\n");
		sbldr.append("Number of deleted records (apprx) : " + mbean.getNumOfDelRecs() + "\n");
		sbldr.append("Number of load : " + mbean.getNumOfLoad() + "\n");
		sbldr.append("Number of sync : " + mbean.getNumOfSync() + "\n");
		sbldr.append("Sync after number of write : " + mbean.getMaxBlkSync() + "\n");
		sbldr.append("Number of writes per sync : " + (mbean.getNumOfActRecs() + mbean.getNumOfDelRecs()) / mbean.getNumOfSync() + "\n");
		sbldr.append("Number of writes per load : " + (mbean.getNumOfActRecs() + mbean.getNumOfDelRecs()) / mbean.getNumOfLoad() + "\n") ;
		sbldr.append("################################# Statistics End #####################################" );	
		System.out.println(sbldr.toString());
	}

	// Create digest from value
	private static byte[] getDigest(byte[] key, byte[] val) {
		if (val.length < 21)
			return val;
		try {
			md.update(key);
			md.update(val);
			return md.digest();
		} finally {
			md.reset();
		}
	}

	// Delete directory
	private static void delDb() throws Exception {
		File rootDir = new File(ROOT_DIR);
		if (!rootDir.exists())
			return;
		// First delete all files in a directory
		for (File f : rootDir.listFiles())
			f.delete();
		// ... Then delete root folder
		rootDir.delete();
	}

	// Open or create database
	private static void openDb() throws Exception {
		// Setup basic properties first
		Properties props = System.getProperties();
		props.setProperty("dataPageSize", "4KB");
		props.setProperty("maxLobSize", "1MB");
		props.setProperty("dataFileSize", "32MB");
		props.setProperty("maxBlkSync", "128");
		// Create or open Database
		db = new KVEngineImp(ROOT_DIR, props, myComp);
	}
    /**
     * Setup test
     * @throws Exception
     */
	public static void setUp() throws Exception {
		rand = ThreadLocalRandom.current();
		md = MessageDigest.getInstance("SHA-1");
		map = new TreeMap<>(myComp);
		ROOT_DIR = System.getProperty("user.dir") + File.separator + "DBTestNew";
		delDb(); // delete db if exist
		dbSize();
	}
	private static void dbSize() {
		try { dbSize = Long.parseLong(System.getProperty("dbSize")) * GB; } 
		catch (NumberFormatException ne) { }
	}
    /**
     * Destroy database
     * @throws Exception
     */
	public static void destroy() throws Exception {
		map.clear();
		map = null;
		db = null;
		delDb();
	}
	/**
	 * Start executing from first to finish
	 * @throws Exception
	 */
	public static void commonTest() throws Exception {
		try {
			openDb();
			firstKey();
			lastKey();
			nextKey();
			prevKey();
			randomGet();
			doItr();
			put();
			delete();
			get();
			printState();
		} finally {
			db.close();
		}
	}
	/**
	 * To support command line run
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			setUp();
			commonTest();
			commonTest();		
		} finally {
			destroy();
		}
	}


}
