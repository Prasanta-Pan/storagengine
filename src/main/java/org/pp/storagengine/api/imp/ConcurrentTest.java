package org.pp.storagengine.api.imp;

import static org.pp.storagengine.api.imp.Util.MB;
import static org.pp.storagengine.api.imp.Util.GB;

import java.io.File;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;

import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;
import org.pp.storagengine.api.KVIterator;
import static org.pp.storagengine.api.imp.KVEngineImp.KVEntryImp;

public class ConcurrentTest extends Thread {
	// Maximum database size to test
	private static long dbSize = 256 * MB;
	// Database root directory
	private static String ROOT_DIR = null;
	// Storage engine
	private static KVEngine db = null;
	// Concurrent navigable map
	private static ConcurrentNavigableMap<byte[], KVEntry> nmap = null;
	// Tread Specific 
	private ThreadLocalRandom rand = null;
	private MessageDigest md = null;
	private volatile String actCmnd = null;	
	
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
	// Setup environment
	public static void setup() throws Exception {
		nmap = new ConcurrentSkipListMap<>(myComp);
		ROOT_DIR = System.getProperty("user.dir") + File.separator + "DBTest";
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
	public static void cleanup() throws Exception {
		nmap.clear();
		nmap = null;
		db = null;
		delDb();
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
	// Value bytes
	private static final int[] vbytes = new int[] 
		{ 1, 3, 4, 8, 1, 3, 4, 8, 12, 16, 20, 30, 40, 80, 75, 
		  100, 110, 140, 150, 210, 240, 300, 320, 350, 511, 575, 
		  590, 600, 630, 710, 810, 819, 890, 901, 1100, 1200, 1350, 
		  3001,3227, 5 * 1024, 20 * 1024, 32 * 1024, 40 * 1024 
		};

	// Random key generator
	private byte[] genKey() {
		//
		byte[] key = new byte[rand.nextInt(1, 35)];
		for (int i = 0; i < key.length; i++)
			key[i] = (byte) rand.nextInt(0, 20);
		return key;
	}
	// Get a random lower mid key
	private byte[] getLowerMidKey() {
		byte[] key = new byte[rand.nextInt(1, 35)];
		for (int i = 0; i < key.length; i++)
			key[i] = (byte) rand.nextInt(3, 10);
		return key;
	}
	// get a random higher mid key
	private byte[] getUpperMidKey() {
		byte[] key = new byte[rand.nextInt(1, 35)];
		for (int i = 0; i < key.length; i++)
			key[i] = (byte) rand.nextInt(13, 20);
		return key;
	}
	// Verify key value pair
	private void verify(byte[] key, byte[] val, KVEntry kvEntry) throws Exception {
		if (!Arrays.equals(key, kvEntry.getKey()))
			throw new RuntimeException("Key's are not matching!\n (" + Arrays.toString(key) + ","
					+ Arrays.toString(kvEntry.getKey()) + ")");
		// Recalculate digest
		byte[] dgst = getDigest(kvEntry.getKey(), kvEntry.getValue());
		if (!Arrays.equals(val, dgst))
			throw new RuntimeException("Digest's are not matching!");
	}
	// Create digest from value
	private byte[] getDigest(byte[] key, byte[] val) {
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
	// Action commands
   	private static final String[] actCmnds = new String[] {
   		"WRITER","GET-1", "GET-2", "GET-3", "ITR",
   		"ITR(START,?)", "ITR(?,END)", "ITR(?,?)" 
   	};
   	
    // Constructors
 	private ConcurrentTest() {}
 	private ConcurrentTest(String actCmnd) { this();  this.actCmnd = actCmnd; }
 	@Override
 	public void run() {
 		try {
 			rand = ThreadLocalRandom.current();
 			md = MessageDigest.getInstance("SHA-1");
 			setName(actCmnd);
 			// Check action commands
			switch (actCmnd) {
				case "WRITER":
					write();
					break;
				case "GET-1":
				case "GET-2":
				case "GET-3":
					get();
					break;
				default:
					itr();
					break;
			}

 		} catch (Exception e) {
 			e.printStackTrace();
 		}
 	}
 	
 	public static void execute() throws Exception {
		// Create a brand new database
		try { 
			Properties props = System.getProperties();
			db = new KVEngineImp(ROOT_DIR, props, myComp);
			// Start all threads
			ConcurrentTest[] thrds = new ConcurrentTest[actCmnds.length];
			for (int i = 0; i < actCmnds.length; i++) {
				thrds[i] = new ConcurrentTest(actCmnds[i]);
				thrds[i].start();
			}
			// Wait for all thread to complete
			StringBuilder sbldr = new StringBuilder();			
			while (alive(thrds)) {
				for (ConcurrentTest thrd : thrds) 
				  sbldr.append(thrd.getMsg() + "\n");
				
				// update statistics periodically
				sbldr.append("\033[");
				sbldr.append(thrds.length);
				sbldr.append("A");
				System.out.print(sbldr.toString());
				sleep(1000);
			}
		} finally { db.close();  }
	}
 	// check if any thread alive or not
 	private static boolean alive(ConcurrentTest[] thrds) {
		for (ConcurrentTest thrd: thrds)
			if (thrd.isAlive())
				return true;
		return false;
	}
    // Display message to the screen
 	private String getMsg() { String tmp = actCmnd; return tmp; }
   	
   	/**
   	 * 
   	 * @throws Exception
   	 */
   	private void write() throws Exception {
   		final int ADD = 0, UPD = 1, DEL = 2;
   		int[] actions = new int[] { ADD, ADD, ADD, UPD, DEL };
   		//
   		byte[] key = null;
   		StringBuilder sbld = new StringBuilder();
   		int curAct = 0, ne = 0, per = 0, upd = 0, del = 0;
   		for (long sz = 0; sz < dbSize;) {
   			// generate action
   			curAct = actions[rand.nextInt(0, actions.length)];
   			switch (curAct) {
   			    case ADD :
   			    	key = genKey();
   			    	if (nmap.containsKey(key))
   			    		continue;
   			    	sz += put(key);
   					ne++; 
   			    	break;
   			    case UPD:
   			    	if (sz < dbSize / 4)
   			    		continue;
   			    	put(nextKey());
   			    	upd++;
   			    	break;
   			    case DEL:
   			    	if (sz < dbSize / 4)
   			    		continue;
   			    	key = nextKey();  
   			    	db.delete(key);
   			    	nmap.remove(key);
   			    	del++;
   			    	break;   			   
   			}
   			per = (int) ((100.0f * sz) / dbSize);
   			// build string to be displayed
   			actCmnd = sbld.append(getName()) // thread name
			   			    .append(":")
			   				.append("ne=" + ne + ",")
			   				.append("per=" + per + ",")
			   				.append("upd=" + upd + ",")
			   				.append("del=" + del)
			   				.toString();
   			sbld.setLength(0);   			
   		}   		
   	}
   	
   	private int put(byte[] key) throws Exception {
		byte[] value = new byte[vbytes[rand.nextInt(0, vbytes.length)]];
		rand.nextBytes(value);
		KVEntry entry = db.put(key, value);
		value = getDigest(key, value);
		entry = new KVEntryImp(entry.getKey(), 
							   value, 
							   entry.isDeleted(), 
							   entry.size(), 
							   entry.timeStamp());
		nmap.put(key, entry);
		return entry.size();
   	}
   	
   	private byte[] nextKey() {
   		byte[] key = genKey();
   		// if contains key
	    if (nmap.containsKey(key))
	    	return key;
	    // get higher key if exist
	    byte[] nk = null;
	    if ((nk = nmap.higherKey(key)) != null)
	    	return nk;
	    // else get lower key of exist
	    if ((nk = nmap.lowerKey(key)) != null)
	       return nk;
	    // get first key
	    throw new RuntimeException("Key Problem")  ;
   	}
   /**
    * 
    * @throws Exception
    */
   	private void get() throws Exception {
   		actCmnd = getName() + ": I am sleeping";
   		sleep(2000);
   		
   		byte[] key = null;
   		KVEntry mEntry = null, kvEntry;
   		StringBuilder sbld = new StringBuilder();
   		int ne = 0, per = 0;
   		for (long sz = 0; sz < dbSize;) {
   			key = nextKey();
   			kvEntry = db.get(key);
   			mEntry = nmap.get(key);
   			if (kvEntry == null || mEntry == null)
   				continue;
   			if (kvEntry.timeStamp() != mEntry.timeStamp())
   				continue;
   			verify(mEntry.getKey(), mEntry.getValue(), kvEntry);
   			sz += kvEntry.size();  ne++;
   			per = (int) ((100.0f * sz) / dbSize);
   			// display message
   			actCmnd = sbld.append(getName()) // thread name
		   			      .append(":")
		   				  .append("ne=" + ne + ",")
		   				  .append("per=" + per)
		   				  .toString();
		    sbld.setLength(0);   	
   		}
   	}
   	/**
   	 * 
   	 * @throws Exception
   	 */
   	private void itr() throws Exception {
   		String cmnd = actCmnd;
   		actCmnd = getName() + ": I am sleeping";
   		sleep(1000);
   		
   		KVIterator kvItr = null;
   		Iterator<KVEntry> itr = null;
   		byte[] key = null, key2 = null;
   		KVEntry mEntry, kvEntry;
   		int ne = 0, itrt = 0, per = 0;;
   		StringBuilder sbld = new StringBuilder();
   		
   		// start iterating
   		for (long sz = 0; sz < dbSize;) {
   			switch (cmnd) {
	   			case "ITR" :
	   				itr = nmap.values().iterator();
	   				kvItr = db.iterator();
	   				break;
	   			case "ITR(START,?)" :
	   				key = getLowerMidKey();
	   				itr = nmap.headMap(key).values().iterator();
	   				kvItr = db.iterator(null,key);
	   				break;
	   			case "ITR(?,END)" :
	   				key = getUpperMidKey();
	   				itr = nmap.tailMap(key).values().iterator();
	   				kvItr = db.iterator(key);
	   				break;
	   			case "ITR(?,?)" :
	   				key = getLowerMidKey();
	   				key2 = getUpperMidKey();
	   				itr = nmap.subMap(key, key2).values().iterator();
	   				kvItr = db.iterator(key, key2);
	   				break;
	   		}
	   		// Start Iterating
			try {
				ne = 0;
				while (itr.hasNext() && kvItr.hasNext()) {
					mEntry = itr.next();
					kvEntry = kvItr.next();
					
					while (mEntry.timeStamp() != kvEntry.timeStamp()) {
						if (mEntry.timeStamp() > kvEntry.timeStamp() && itr.hasNext())
							mEntry = itr.next();
						else if (mEntry.timeStamp() < kvEntry.timeStamp() && kvItr.hasNext())
							kvEntry = kvItr.next();
						else
							throw new RuntimeException("Something wrong");
					}
					verify(mEntry.getKey(), mEntry.getValue(), kvEntry);
					sz += kvEntry.size();
					per = (int) ((100.0f * sz) / dbSize);
					ne++;
				}
			} finally {
				kvItr.close();
				itrt++;
			}
			// display message
   			actCmnd = sbld.append(getName()) // thread name
		   			      .append(":")
		   				  .append("ne=" + ne + ",")
		   				  .append("per=" + per + ",")
		   				  .append("itr=" + itrt)
		   				  .toString();
		    sbld.setLength(0);   			
   		}
   	}
   	
   	public static void main(String[] args) throws Exception {
   		try {
   			setup();
   			execute();
   		} finally {
   			cleanup();
   		}
   	}
	
}
