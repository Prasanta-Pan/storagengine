package org.pp.storagengine.api.imp;

import static java.lang.Math.abs;
import static java.util.Arrays.binarySearch;
import static org.pp.storagengine.api.imp.KVLog.print;
import static org.pp.storagengine.api.imp.KVLog.println;
import static org.pp.storagengine.api.imp.Util.atoRename;

import java.io.EOFException;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;
import org.pp.storagengine.api.KVIterator;
import org.pp.storagengine.api.StatisticsMXBean;

public class KVEngineImp extends AbstractFileHandler implements KVEngine {
	/** Root node... */
	private volatile RootNode rNode;
	/** Right most block */
	private volatile long right;
	/** Comparators... */
	private final EComparator eComp ;		
	/** Write lock, to allow one writer at time.. */
	private final Lock wLoc = new ReentrantLock(true); 
	
	/** data file and block number */
	private int dataFileNum = 0; 
	private int dBlkNo = 1;
	/** Current write counter */
	private int curBlkSync = 0;
	/** Tree log file */
	private RandomAccessFile tlFile;	
	/** File Cache */
	private final LRUCache<IntCacheEntry, RandFileCacheEntry> fCache;
	/** Block locker */
	private final KeyLocker<Long> kLocker = new KeyLocker<>(64);
	/** Lock file to support crash detection */
	private File lock = null;
	/** Load Indicator */
	private boolean load = true;
	
	public KVEngineImp(String fileOrDir, Properties options, Comparator<byte[]> comp) throws Exception {
		/** Comparator check */
		if (comp == null) throw new Exception("Comparator can not be null");
		eComp = new EComparator(comp);		
		/** Initialise SysContext & Statistics */
		super.ctx = new SysContext(fileOrDir, options);
		super.stat = Statistics.register(ctx);		
		/** Initialise file cache and auto sync */
		fCache = new LRUCache<>(ctx.getMaxBlkSync(), -1L);
		/** load BTree now.... */
		long time = System.currentTimeMillis();
		lock = initBtree();
		// indicate that loading is over
		load = false;
		// find right most block
		right = findLastBlock(rNode.root, rNode.ht);
		// print loading time
		println("Time to Load: " + (System.currentTimeMillis() - time));				
	}
	// INIT and load B+Tree
	private File initBtree() throws Exception {
		/** INIT root */
		KVEntryImp tmp = new KVEntryImp(null,0L,-1);
		NodeEntry nEntry = new NodeEntry(-1L, new KVEntryImp[] { tmp });
		rNode = new RootNode(new Node(nEntry), 0);
		
		/** Open lock file if exist */
		File lock = new File(ctx.getRootDir() + File.separator + ".lock");
		/** Check if tree log file exist or not */
		File tlf = new File(ctx.getRootDir() + File.separator + ".tlog");
		// database don't exist, create a new one
		if (!tlf.exists()) { 
			// INIT data block
			ByteBuffer buf = ByteBuffer.allocate(ctx.getBlockSize());
			new NodeEntry().serialize(buf);
			writeBlk(0L, buf.array());
			// create TLF file
			tlFile = new RandomAccessFile(tlf, "rw");
			// create lock file
			lock.createNewFile();
			stat.numOfDataFiles = dataFileNum + 1;
			return lock;			
		}
		// load file and block number
		long ref = loadFileAndBlkNum();
		// check the length of TLF file
		long tlfLen = tlf.length();
		/**
		 *  Check if lock file exist or not.
		 *  If lock file exist means a crash was occurred.
		 *  in that case a fresh TLF file will be created discarding blocks which doesn't exists
		 */
		File tmpTlf = null;
		RandomAccessFile tmpRan = null;
		if (lock.exists()) {
			println("A crash might have happened...");
		    tmpTlf = new File(ctx.getRootDir() + File.separator + ".tmp");
			tmpTlf.delete();
			// create a new one
			tmpTlf.createNewFile();
			   // open TMP file
			tmpRan = new RandomAccessFile(tmpTlf, "rw");			
		}
		// open TLF file now in read write mode
		tlFile = new RandomAccessFile(tlf, "rw");
	    // loading message to the user		
		String msg = "Database loading in progress....";	
		
		// Start loading database now
		byte[] bytes = null;
		Object[] entries = null;
		long sz = 0; int prog = 0, lastProg = 0;
		// read TLF entry one by one
		while ((entries = readEntry()) != null) {
			tmp = (KVEntryImp) entries[0];	
			// check if entry reference valid
			if ( (long) tmp.value >= ref)
				break;
			// build tree
			buildTree(tmp);
			// get entry bytes as well
			bytes = (byte[]) entries[1];
			// rewrite to TMP TLF 
			if (tmpTlf != null)
				tmpRan.write(bytes);
			// calculate progress
			sz += bytes.length;
			prog = (int) ((100.0f * sz) / tlfLen);
			if (prog > lastProg) {
				print(msg + prog + "%\r");
				lastProg = prog;
			}
		}		
		
		// close TMP TLF and atomic rename
		if (tmpTlf != null) {
			// close TMP TLF
			close(tmpRan);
			// Close TLF as well
			tlFile.close();
			// atomic rename
			atoRename(tmpTlf.getAbsolutePath(),tlf.getAbsolutePath());
			// Re open tree log file and seek to end
			tlf = new File(ctx.getRootDir() + File.separator + ".tlog");
			tlFile = new RandomAccessFile(tlf, "rw");
			tlFile.seek(tlFile.length());
		}	
		// update statistics and return lock file
		stat.numOfDataFiles = dataFileNum + 1;
		lock.createNewFile();
		return lock;
	}		
	/**
	 * Load file and block number and return last reference
	 * @throws Exception
	 */
	private long loadFileAndBlkNum() throws Exception {
		Object[] objs = latestFile(dfPrefix, dfPostfix);
		dataFileNum = (int) objs[1];
		try (RandomAccessFile rFile = new RandomAccessFile( (File) objs[0],"r")) {
			dBlkNo = (int) (rFile.length() / ctx.getBlockSize());
			println("Data File / BlkNo: " + dataFileNum + "/" + dBlkNo);
			return (long) dataFileNum << 32 | dBlkNo & 0xFFFFFFFFL;			
		}		
	}
	/**
	 * Supporting method to read entry from TLF file
	 * @return
	 * @throws Exception
	 */
	private Object[] readEntry() throws Exception {
		try {
			// get entry length
			int len = tlFile.readInt();
			// read entire entry
			byte[] data = new byte[len];
			tlFile.read(data, 4, len - 4);
			// update bytes
			ByteBuffer bBuffer = ByteBuffer.wrap(data);
			bBuffer.putInt(len).position(0);
			// DE-serialise entry
			KVEntry tmp = KVEntryImp.dSerialize(bBuffer);			
			return new Object[] { tmp, data };
		} catch (EOFException e) {  }
		return null;
	}
	/**
	 * Generate disk reference number
	 * @param nb
	 * @return
	 */
	private long genDiskRefNo(int nb) {
		// Check if new file is required
		if (dBlkNo + nb > ctx.getMBlockFile()) {
			dataFileNum++;
			dBlkNo = 0;
			stat.numOfDataFiles++;
		}
		// generate disk reference number
		long ref = (long) dataFileNum << 32 | dBlkNo & 0xFFFFFFFFL;
		// update block number
		dBlkNo += nb;
		// return reference number
		return ref;
	}	
	/**
	 * Common B+ Tree building method for both during bulk loading and runtime.
	 * @param entry
	 * @param load
	 * @throws Exception
	 */
	void buildTree(KVEntryImp entry) throws Exception {
		/** Update tree now... */
		Node node = insertEntry(rNode.root, entry, rNode.ht);
		/** create new root... */
		if (node != null) {
			// Existing root
			KVEntryImp eR = new KVEntryImp(null, rNode.root, rNode.size());
			// New root
			KVEntryImp nR = new KVEntryImp(node.firstKey(), node, node.size());
			NodeEntry newRoot = new NodeEntry(-1, new KVEntryImp[] { eR, nR });
			rNode = new RootNode(new Node(newRoot), rNode.ht + 1);
			stat.ht = rNode.ht;
			stat.htMap.put(stat.ht, 0);			
		}
	}
	/**
	 * Insert entry into the tree
	 * @param node
	 * @param entry
	 * @param htl
	 * @param load
	 * @return
	 * @throws Exception
	 */
	private Node insertEntry(Node node, KVEntryImp entry, int htl) throws Exception {
		NodeEntry nEntry = node.getNodeEntry();
		// find index
		int index = findIndex(nEntry, entry);
		/**
		 * recursively reach at level 0
		 */
		if (htl > 0) {
			Node newNode = insertEntry(nEntry.getNode(index), entry, htl - 1);
			if (newNode == null) return null;
			entry = new KVEntryImp(newNode.firstKey(), newNode, newNode.size());
		} 
		/**
		 * if not bulk loading
		 */
		else if (!load) { 
			entry = updateBlock(nEntry.entries[index], entry);
			if (entry == null) return null;
		} 
		/**
		 * adjust brunch entry
		 */
		return adjustEntry(node, ++index, entry, htl);		
	}
    /**
     * Update block. Just append if the block is not full or fetch write and split
     * @param bEntry
     * @param entry
     * @return
     * @throws Exception
     */
	private KVEntryImp updateBlock(KVEntryImp bEntry, KVEntryImp entry) throws Exception {
		/**
		 * Check if entry is LOB or Not.
		 */
		entry = checkLob(entry);
		/**
		 *  Load raw data block and get the block size
		 */
		Long blkNo = (long) bEntry.value;
		byte[] data = readBlk(blkNo, ctx.getBlockSize());
		ByteBuffer buf = ByteBuffer.wrap(data); // get a byte buffer
		int size = buf.getInt(); // overall size
		int sz = entry.sSize(); // entry size

		/**
		 * Check if the entry can be appended to block or not.
		 * If size permit, append data and persist block
		 */
		if (size + sz < ctx.getBlockSize()) {
			buf.position(size);
			entry.serialize(buf);
			buf.putInt(0, size + sz);
			lockAndWrite(blkNo, data);			
			return null;
		}
		/**
		 * If block is full, no more appending.
		 * Get sorted entries as well as unsorted.
		 * Merge it To gather.
		 */
		buf.clear();
		NodeEntry nEntry = NodeEntry.loadSorted(buf, blkNo);
		nEntry = merge(NodeEntry.loadUnsorted(buf, entry), nEntry);
		/**
		 * If block size is not sufficient enough to split.
		 */
		if (nEntry.size < ctx.getBlockSize()) {
			nEntry.serialize(buf);
			lockAndWrite(blkNo, data);					
			return null;
		}
		/**
		 * It's time to split the blocks. First lock next and splitting block.
		 * Than persist the new block followed by splitting block.
		 * Update previous link of next block.
		 */
		Node node = new Node(nEntry);
		Node newNode = split(node, genDiskRefNo(1), -1);
		// lock both blocks for split and link updates
		Long nBlkId = newNode.nextBlkNo();
		kLocker.lock(nBlkId);
		kLocker.lock(blkNo);
		try {
			/**
			 * Serialise new block first
			 */
			nEntry = newNode.nEntry;
			nEntry.serialize(buf);
			writeBlk(nEntry.blkNo, data);
			// get first key
			entry = new KVEntryImp(nEntry.fkey(), nEntry.blkNo, nEntry.size);
			/**
			 * Now serialise splitted block
			 */
			buf.clear();
			nEntry = node.nEntry;
			nEntry.serialize(buf);
			writeBlk(nEntry.blkNo, data);
			
			/**
			 * if the new block is the last block
			 */
			if (nBlkId == -1L) {
				right = newNode.getBlkNo();
			}
			/**
			 * else Update previous link of next block
			 */
			else {
				byte[] dataNext = readBlk(nBlkId, ctx.getBlockSize());
				NodeEntry.setPrevLink(newNode.getBlkNo(), ByteBuffer.wrap(dataNext));
				writeBlk(nBlkId, dataNext);
				curBlkSync++;
				stat.numOfLoad++;
			}
			
		} finally {
			kLocker.unlock(nBlkId);
			kLocker.unlock(blkNo);
		}		
		// write tlf entry
		tlFile.write(entry.serialize(null).array());
		curBlkSync += 2;
		stat.numOfLoad++;
		return entry.getACopy();
	}	
	
	/**
	 * Lock block and write
	 * @param bno
	 * @param data
	 * @throws Exception
	 */
	private void lockAndWrite(Long bNo, byte[] data) throws Exception {
		// lock block first
		kLocker.lock(bNo);
		try {
			// write block now
			writeBlk(bNo, data);
			// update block sync
			curBlkSync++;
		} finally {
			kLocker.unlock(bNo);
		}
	}
	
	/**
	 * Read entire block with lock
	 * @param bNo
	 * @return
	 * @throws Exception
	 */
	private byte[] lockAndRead(Long bNo) throws Exception {
		// lock block first
		kLocker.lock(bNo);
		try {
			// read block
			return readBlk(bNo, ctx.getBlockSize());
		} finally {
			kLocker.unlock(bNo);
		}
	}
		
	/**
	 *  Check if the ENTRY is LOB
	 * @param entry
	 * @return
	 * @throws Exception
	 */
	private KVEntryImp checkLob(KVEntryImp entry) throws Exception {
		// Check if LOB
		if (entry.size >= ctx.getBlockSize() / 2) {
			// check if too big LOB
			if (entry.size > ctx.getMaxLobSize())
				throw new RuntimeException("Too Big Lob");
			// calculate number of blocks
			int nb = entry.size / ctx.getBlockSize();
			// check if odd block number
			if (entry.size % ctx.getBlockSize() != 0)
				nb++;
			// generate blocks
			long ref = genDiskRefNo(nb);			
			// Serialise data
			ByteBuffer buf = ByteBuffer.allocate(nb * ctx.getBlockSize());
			entry.serialize(buf);
			// Write to the storage
			writeBlk(ref, buf.array());
			// Update entity with reference
			entry.value = ref;			
			// update block counter
			curBlkSync += nb;			
		}
		// Return updated entry
		return entry;
	}
	/**
	 * Load LOB if applicable
	 * @param entry
	 * @return
	 * @throws Exception
	 */
	private KVEntryImp loadLob(KVEntryImp entry) throws Exception {
		// Check if LOB
		if (entry.size >= ctx.getBlockSize() / 2) {
			long ref = (long) entry.value;
			// calculate number of blocks
			int nb = entry.size / ctx.getBlockSize();
			// check if odd block number
			if (entry.size % ctx.getBlockSize() != 0)
				nb++;
			// read LOB now 
			byte[] data = readBlk(ref, nb * ctx.getBlockSize());
			// get byte buffer
			ByteBuffer buf = ByteBuffer.wrap(data);
			// Get it d serialise
			entry = KVEntryImp.dSerialize(buf);				
		}
		// Return updated entry
		return entry;
	}
	/**
	 * Block writer
	 * @param blkNo
	 * @param data
	 * @throws Exception
	 */
	private void writeBlk(long blkNo, byte[] data) throws Exception {
		// find file number from reference
		int fileNo = (int) (blkNo >> 32);
		// find block number from reference number as well
		int bNo = (int) blkNo;
		// create a cache key
		IntCacheEntry key = new IntCacheEntry(fileNo);
		// get cache value
		RandFileCacheEntry rf = fCache.get(key);
		if (rf == null) {
			// Create file name
			String fName = getFileName(fileNo,dfPrefix,dfPostfix);
			// Create or open file
			rf = new RandFileCacheEntry(new RandomAccessFile(new File(fName), "rw"));
			fCache.put(key, rf);			
		}
		// seek
		rf.getRf().seek(ctx.getBlockSize() * bNo);
		// write
		rf.getRf().write(data);			
	}
	/**
	 * Sync all open files and TLF
	 */
	private void syncFiles() throws Exception {
		// get current time stamp
		long tt = System.currentTimeMillis();
		// First sync and close all open files		
		if (fCache.size() > 0) {
			RandFileCacheEntry tmpF = null;
			while ((tmpF = fCache.removeOldest()) != null)
				close(tmpF.getRf());					
		}
		// Now sync TLF
		sync(tlFile);
		// Update statistics
		long t = (System.currentTimeMillis() - tt);
		if (t > stat.maxSyncTime)
			stat.maxSyncTime = t;
		// update number of sync statistics
		stat.numOfSync++;
		// update sync time
		stat.lastSyncTime = tt;		
	}
	
	/**
	 * create new entry in branch node
	 * @param node
	 * @param pointer
	 * @param entry
	 * @param ht
	 * @return
	 * @throws Exception
	 */
	private Node adjustEntry(Node node, int pointer, KVEntryImp entry, int ht) throws Exception {
		// retrieve corresponding node entry from node
		NodeEntry nEntry = node.getNodeEntry();
		// create copies of existing KVEntry
		KVEntryImp[] tmp = new KVEntryImp[nEntry.Len() + 1];
		// copy it
		System.arraycopy(nEntry.entries, 0, tmp, 0, nEntry.Len());
		// find possible insertion point
		for (int j = tmp.length - 1; j > pointer; j--)
			tmp[j] = tmp[j - 1];
		// insert new entry
		tmp[pointer] = entry;
		// calculate new size
		int size = nEntry.size + entry.sSize();
		// create a new node entry
		NodeEntry eLocal = new NodeEntry(nEntry.blkNo,tmp,size,nEntry.next, nEntry.prev);
		// Atomically set new Node Entry
		node.setNodeEntry(eLocal);
		// update statistics
		stat.numOfBranchEntry++;
		// do split if size go beyond block size
		if (size >= ctx.getBlockSize())
			return split(node, -1, ht);
		// return null if no split
		return null;
	}
    /**
     * Split node and return the newly created node
     * @param src
     * @param blkNo
     * @param ht
     * @return
     * @throws Exception
     */
	private Node split(Node src, long blkNo, int ht) throws Exception {
		// New node to be return
		Node newNode = new Node();
		// get source entry of source node
		NodeEntry srcNEntry = src.getNodeEntry();
		// get entry length of source node entry
		int i = srcNEntry.Len() - 1;
		// calculate split size
		int elmntLeft = 0, elmntRight = 0, piv = ctx.getBlockSize() / 2, size = NodeEntry.INIT_SIZE;
		for (; size < piv && elmntLeft < i; elmntLeft++)
			size += srcNEntry.entries[elmntLeft].sSize();	
		// adjust entries for the splitted entries
		elmntRight = srcNEntry.Len() - elmntLeft;		
		// allocate new source node entry
		NodeEntry newSrc = new NodeEntry(srcNEntry.blkNo, new KVEntryImp[elmntLeft],0);
		// copy first half of the entries
		System.arraycopy(srcNEntry.entries, 0, newSrc.entries, 0, elmntLeft);
		// allocate new node entry for the new node
		NodeEntry newNEntry = new NodeEntry(blkNo, new KVEntryImp[elmntRight], NodeEntry.INIT_SIZE);
		// copy remaining entries to new node entries
		System.arraycopy(srcNEntry.entries, elmntLeft, newNEntry.entries, 0, elmntRight);
        // adjust size
		newSrc.size = size;
		newNEntry.size += (srcNEntry.size - size);
        // Setup links for new node
		newNEntry.next = srcNEntry.next;
		newNEntry.prev = src;
		// setup links for the new source
		newSrc.next = newNode;
		newSrc.prev = srcNEntry.prev;
		
		// Atomically set src and new node
		newNode.setNodeEntry(newNEntry);
		src.setNodeEntry(newSrc);
		// update statistics
		stat.htMap.put(ht,stat.htMap.get(ht) + 1);
		return newNode;		
	}
	
	/**
	 * To find node node in the tree
	 * @param nEntry
	 * @param entry
	 * @return
	 */
	private int findIndex(NodeEntry nEntry, KVEntryImp entry) {
		int index = nEntry.fkey() != null ? 0 : 1;
		index = binarySearch(nEntry.entries, index, nEntry.Len(), entry, eComp);
		if (index >= 0) return index;
		index = abs(index) - 2;
		return index < 0 ? 0 : index;
	}	
	/**
	 * Common method to support both Get and Iterator.
	 * @param node
	 * @param htl
	 * @param entry
	 * @return
	 * @throws Exception
	 */
	private KVEntryImp findEntry(Node node, int htl, KVEntryImp entry) throws Exception {
		NodeEntry nEntry = node.getNodeEntry();
		int indx = findIndex(nEntry, entry);
		/**
		 * In case node split happened before parent was update
		 */
		if (indx == nEntry.Len() - 1 && nEntry.next != null) {
			NodeEntry nLocal = nEntry.nextNode().getNodeEntry();
			if (eComp.compare(entry, nLocal.entries[0]) >= 0)
				return findEntry(nEntry.nextNode(), htl, entry);
		}
		if (htl > 0)
			return findEntry(nEntry.getNode(indx), htl - 1, entry);
		// Get the block number
		Long blNo = nEntry.getBlk(indx);
		/**
		 * In case node was just split in time.Iterate through the next block as well
		 */
		do {
			// load entries
			nEntry = loadAndMerge(blNo);
			// find entry now
			indx = binarySearch(nEntry.entries, 0, nEntry.Len(), entry, eComp);
		} while (indx < 0 && (abs(indx) - 1) == nEntry.Len() && (blNo = nEntry.nextBlk()) != -1);
		/**
		 * To support iterator only
		 */
		if (entry.dMarker) {
			entry.value = nEntry;
			entry.size = indx;
			return entry;
		}
		/**
		 * Return entry if found or null
		 */
		return indx >= 0 && !nEntry.entries[indx].dMarker ? nEntry.entries[indx] : null;
	}
	
	/**
	 * Merge sorted and unsorted entries together 
	 * @param uNode
	 * @param sNode
	 * @param eEntry
	 * @return
	 * @throws Exception
	 */
	private NodeEntry merge(NodeEntry uNode, NodeEntry sNode) throws Exception {
		int sLen = sNode.Len(); // Sorted entries length
		int ulen = uNode.Len(); // Unsorted entries length
		// Sorted + Unsorted Entries
		KVEntryImp[] newEntries = null; // 
		// If sorted / unsorted both entries exist
		if (sLen > 0 && ulen > 0) { 
			newEntries = new KVEntryImp[sLen + ulen];
			// Copy existing entries to new entries
			System.arraycopy(sNode.entries,0,newEntries,0,sLen);
		} 
		// If only unsorted entries (First block only)
		else if (ulen > 0)  
			newEntries = new KVEntryImp[ulen];	
		// just return if their is no unsorted block
		else  
			return sNode;
		// do the merging now
		int index = 0;
		for (KVEntryImp entry: uNode.entries) {			
			index = binarySearch(newEntries,0,sLen,entry,eComp);
			if (index > -1) {
				newEntries[index] = entry;
				continue;
			}
			// adjust entry
			index = abs(index) - 1;
			for (int j = sLen; j > index; j--)
				newEntries[j] = newEntries[j - 1];
			newEntries[index] = entry;
			sLen++;
		}
		// In case some entries are deleted
		if (sLen < newEntries.length) {
			KVEntryImp[] tmpn = new KVEntryImp[sLen];
			System.arraycopy(newEntries,0,tmpn,0,sLen);
			newEntries = tmpn;
		}
		return new NodeEntry(sNode.blkNo,newEntries, sNode.next, sNode.prev);
	}
	
	/**
	 *  Load block, deserialize, sort and merge
	 * @param blkId
	 * @return
	 * @throws Exception
	 */
	private NodeEntry loadAndMerge(Long blkId) throws Exception {
		// read block
		byte[] data = lockAndRead(blkId);
		// Get byte buffer
		ByteBuffer buf = ByteBuffer.wrap(data);
		// Load sorted list
		NodeEntry nEntry = NodeEntry.loadSorted(buf, blkId);
		// load unsorted list now
		return merge(NodeEntry.loadUnsorted(buf, null), nEntry);
	}
	
	/**
	 * Load the last block
	 * @param blkId
	 * @return
	 * @throws Exception
	 */
	private NodeEntry loadLastBlock() throws Exception {
		/**
		 *  To ensure we are dealing with right mode block
		 */
		Long nextId = right, blkId;
		ByteBuffer buf = null;		
		do {
			blkId = nextId;
			// lock block id
			kLocker.lock(blkId);
			try {
				// read block
				buf = readBlk(blkId);
				// read next link to ensure we are dealing with last block only
				nextId = NodeEntry.getNextLink(buf);
			} finally {
				kLocker.unlock(blkId);
			}
		} while (nextId != -1L);
		
		// Load sorted list
		NodeEntry nEntry = NodeEntry.loadSorted(buf, blkId);
		// load unsorted list now
		return merge(NodeEntry.loadUnsorted(buf, null), nEntry);		
	}
	
	/**
	 * Find the right most block
	 * @param n
	 * @param ht
	 * @return
	 */
	private long findLastBlock(Node n, int ht) {
		/**
		 * we are only interested in the last node
		 */
		NodeEntry nEntry = n.getNodeEntry();
		if (ht > 0)
			return findLastBlock(nEntry.getNode(nEntry.Len() - 1), ht - 1);
		/**
		 *  get the last block
		 */
		return nEntry.getBlk(nEntry.Len() - 1);	
	}
		
	/**
	 * Load previous block data
	 * @param blkId
	 * @return
	 * @throws Exception
	 */
	private NodeEntry loadPrevBlk(Long blkId) throws Exception {
		ByteBuffer buf = null;
		long prevId = -1L;
		/**
		 * Lock block and than read previous block if exist
		 */
		kLocker.lock(blkId);
		try {
			// read block
			buf = readBlk(blkId);
			// get the previous block number
			prevId = NodeEntry.getPrevLink(buf);
			// we already hit the dead end
			if (prevId == -1L)
				return null;
			// read previous block now
			buf = readBlk(prevId);			
		} finally {
			kLocker.unlock(blkId);			
		}
		// Load sorted list
		NodeEntry nEntry = NodeEntry.loadSorted(buf, prevId);
		// load unsorted list now
		return merge(NodeEntry.loadUnsorted(buf, null), nEntry);		
	}
	
	@Override
	public void sync() throws Exception {
		wLoc.lock();
		try { syncFiles(); } 
		finally { wLoc.unlock(); }
	}

	@Override
	public StatisticsMXBean getStatistics() {
		// Return statistics MBean
		return stat;
	}

	@Override
	public KVEntry get(byte[] key) throws Exception {
		KVEntryImp entry = new KVEntryImp(key, null);
		RootNode lNode = rNode;
		entry =  findEntry(lNode.root, lNode.ht , entry);
		return entry != null ? loadLob(entry) : null;
	}

	@Override
	public KVEntry delete(byte[] key) throws Exception {
		return put(key, null, true);		
	}

	@Override
	public KVEntry put(byte[] key, byte[] value) throws Exception {
		return put(key,value,false); 		
	}
	
	/**
	 * common method for add/update/delete operations
	 * @param key
	 * @param val
	 * @param del
	 * @return
	 * @throws Exception
	 */
	private KVEntry put(byte[] key, byte[] val, boolean del) throws Exception {
		// acquire write lock first 
		wLoc.lock();
		try {
			// Create Entry
			KVEntryImp entry = new KVEntryImp(key, val, del);
			/** Update tree now... */
			buildTree(entry);
			/** check if time for issue a sync... */
			if (curBlkSync > ctx.getMaxBlkSync()) {
				syncFiles();
				curBlkSync = 0;
			}
			// set value back to value in case it was a LOB
			entry.value = val;
			// update size statistics
			stat.apprxSize += entry.size;
			// update number of record statistics
			if (!del) stat.numOfActRecs++;
			// update number of deleted record statistics
			else stat.numOfDelRecs++;
			return entry;			
		} finally {
			wLoc.unlock();
		}		
	}
	
	@Override
	public KVIterator iterator() throws Exception {
		return new IteratorImp(null, null, false);
	}
	
	@Override
	public KVIterator iterator(boolean rev) throws Exception {
		return new IteratorImp(null, null, rev);
	}

	@Override
	public KVIterator iterator(byte[] start) throws Exception {
		return new IteratorImp(start, null, false);
	}
	
	@Override
	public KVIterator iterator(byte[] start, boolean rev) throws Exception {
		return new IteratorImp(start , null , rev);
	}

	@Override
	public KVIterator iterator(byte[] start, byte[] end) throws Exception {
		return new IteratorImp(start, end, false);
	}
	
	@Override
	public KVIterator iterator(byte[] start, byte[] end, boolean rev) throws Exception {
		return new IteratorImp(start, end, rev);
	}

	@Override
	public KVEntry firstEntry() throws Exception {
		// return first key
		return getKey(null, false, false);
	}	

	@Override
	public KVEntry lastEntry() throws Exception {
		// return last key
		return getKey(null, true, false);
	}	
	
	@Override
	public KVEntry nextEntry(byte[] key) throws Exception {
		// return next key if exist
		return getKey(key, false, true);
	}
	
	@Override
	public KVEntry prevEntry(byte[] key) throws Exception {
		// return previous key if exist
		return getKey(key, true, true);
	}
	/**
	 * Common method for next/previous/first/last key
	 * @param key
	 * @param rev
	 * @return
	 * @throws Exception
	 */
	private KVEntry getKey(byte[] key, boolean rev, boolean validateKey) throws Exception {
		/**
		 * validate key if required
		 */
		if (validateKey && key == null)
			throw new RuntimeException("Null key is not acceptable!");
		/**
		 * Get an iterator based on direction
		 */
		KVIterator itr = iterator(key, rev);
		try {
			while (itr.hasNext()) {
				KVEntry entry = itr.next();
				/**
				 * if first or last key was requested
				 */
				if (key == null)
					return entry;
				/**
				 * otherwise
				 */
				else if (!Arrays.equals(entry.getKey(), key))
					return entry;
			}
		} finally {
			itr.close();
		}
		return null;
	}
	
	@Override
	public void close() throws Exception {
		// Hold the write lock
		wLoc.lock();
		try {
			// sync all open files
			syncFiles();
			// Close tree log file
			close(tlFile);
			// delete lock file
			lock.delete();
			// Unregister statistics
			stat.unregister();	
			ctx = null;
			rNode = null;			
		} finally {
			wLoc.unlock();
		}
	}
	
	/**
	 * Internal comparator for entry comparison
	 * @author prasantsmac
	 *
	 */
	private static final class EComparator implements Comparator<KVEntryImp> {
		private Comparator<byte[]>  comptr = null;
		private EComparator(Comparator<byte[]>  comptr) {
			this.comptr = comptr;
		}
		@Override
		public int compare(KVEntryImp o1, KVEntryImp o2) {
			return comptr.compare(o1.key, o2.key);
		}
	}
	
	/**
	 * A place holder of Node
	 * @author pan.prasanta@gmail.com
	 *
	 */	
	private static final class RootNode {
		private Node root;
		private int ht;
		
		private RootNode(Node root, int ht) {
			this.root = root;
			this.ht = ht;			
		}
		private int size() { return root.nEntry.size; }
	}
	
	/**
	 * Node is a place holder of Node Entry
	 * @author pan.prasanta@gmail.com
	 *
	 */
	static final class Node {
		/**
		 * Atomic update of NodeEntry
		 */
		volatile NodeEntry nEntry = null;
		/**
		 * Set new NodeEntry atomically
		 * @param newEntry
		 */		
		void setNodeEntry(NodeEntry newEntry) { this.nEntry = newEntry;	}
		/**
		 * Return current NodeEntry
		 * @return
		 */
		NodeEntry getNodeEntry() { return nEntry; }
		/**
		 * Get disk block number associated with this Node
		 * @return
		 */
		long getBlkNo() { return nEntry.blkNo ; }	
		/**
		 * Return next block number
		 * @return
		 */
		long nextBlkNo() {
			Object next = nEntry.next;
			return next != null ? (long) next : -1L;
		}
		/**
		 *  Return NodeEntry Size
		 *  @return
		 */
		int size() { return nEntry.size; }
		/**
		 * Get First key of The node entry
		 * @return
		 */
		byte[] firstKey() { return nEntry.entries[0].key; }
		
		// Constructors 
		Node() { }
		Node(NodeEntry nEntry) { this.nEntry = nEntry; 	}	
	}
	/**
	 * Node Entry holds list of entries
	 * @author pan.prasanta@gmail.com
	 *
	 */
	static final class NodeEntry {
		/**
		 * Minimum size of Node Entry
		 */
		static final int INIT_SIZE = 24;
		/**
		 * Physical disk block number
		 */
		long blkNo;
		/**
		 * Number of KV entries
		 */
		KVEntryImp[] entries;
		/**
		 * Link to next and previous node entry
		 */
		Object next, prev;
		/**
		 * Initial size of the node entry
		 */
		int size = INIT_SIZE;
		
        /**
         * Create a empty node entry
         */
		private NodeEntry() { }
		/**
		 *  Node Entry with just block number
		 * @param blkNo
		 */
		private NodeEntry(long blkNo) { this.blkNo = blkNo; }		
		/**
		 * Node entry with block number and entries
		 * @param bNo
		 * @param entries
		 */
		private NodeEntry(long bNo, KVEntryImp[] entries) {
			this.blkNo = bNo;
			this.entries = entries;
			this.size = calcSize();
		}		
		/**
		 * Block number entries and size in advance
		 * @param bNo
		 * @param entries
		 * @param size
		 */
		private NodeEntry(long bNo, KVEntryImp[] entries, int size) {
			this.blkNo = bNo;
			this.entries = entries;
			this.size = size;
		}
		/**
		 * 
		 * @param bNo
		 * @param entries
		 * @param next
		 * @param prev
		 */
		private NodeEntry(long bNo, KVEntryImp[] entries, Object next, Object prev) {
			this(bNo,entries);
			this.next = next;
			this.prev = prev;
		}
		/**
		 * Block number, entries and size in advance and next/prev link
		 * @param bNo
		 * @param entries
		 * @param size
		 * @param next
		 * @param prev
		 */
		private NodeEntry(long bNo, KVEntryImp[] entries, int size, Object next, Object prev) {
			this(bNo, entries, size);
			this.next = next;
			this.prev = prev;
		}
		// [size(4)][sorted len(4)][next blk][prev blk][....]
		int calcSize() {
			int sSz = INIT_SIZE;
			if (entries == null)
				return sSz;
			for (int i = 0; i < entries.length; i++)
				 sSz += entries[i].sSize();			
			return sSz;
		}
		// return entries length
		int Len() { return entries.length; }	
		// return first key
		byte[] fkey() { return entries[0].key; }
		// return key at index
		byte[] key(int index) {  return entries[index].key; }
		// return value at index
		byte[] value(int index) { return (byte[]) entries[index].value; }
		
		// [size(4)][sorted len(4)][next blk][prev blk][....]
		// set previous block link
		static ByteBuffer setPrevLink(long blkNo, ByteBuffer prevBlkBuf) {
			prevBlkBuf.putLong(16, blkNo);
			return prevBlkBuf;
		}
		// return previous link
		static long getPrevLink(ByteBuffer buf) {
			return buf.getLong(16);
		}
		// return next block number
		static long getNextLink(ByteBuffer buf) {
			return buf.getLong(8);
		}
		
		// [size(4)][sorted len(4)][next blk][prev blk][....]
		void serialize(ByteBuffer buf) {
			// update block info first
			buf.putInt(size) // Overall size
			   .putInt(size) // Sorted size
			   .putLong(getBlkNo(next)) // next blk num
			   .putLong(getBlkNo(prev)); // prev blk num	
			// if no entries to serialise
			if (entries == null)
				return;
			// Serialise entries
			for (KVEntryImp entry : entries) 
				entry.serialize(buf);							
		}
		// 
		private long getBlkNo(Object obj) {
			if (obj == null) return -1L;
			else if (obj instanceof Long) return (long) obj;
			else return ((Node) obj).getBlkNo(); 
		}		
		/** Return Node by Index */
		Node getNode(int index) { return (Node) entries[index].value; }
		/** Return block no by Index */
		Long getBlk(int index) { return (Long) entries[index].value; }
		// Next Node
		Node nextNode() { return (Node) next; }
		// Next Block
		long nextBlk() { return (long) next; }	
		// return previous block
		long prevBlk() { return (long) prev; }
	   		
		// [size(4)][sorted len(4)][next blk][prev blk][......]
		static NodeEntry loadSorted(ByteBuffer buf, long blkNo) {
			buf.getInt(); // entire block size
			NodeEntry nEntry = new NodeEntry(blkNo);
			nEntry.size = buf.getInt(); // Sorted size
			nEntry.next = buf.getLong(); // next block
			nEntry.prev = buf.getLong(); // previous block
			buf.limit(nEntry.size); // Set limit to sorted size
			loadEntries(buf, nEntry, null);
			buf.clear();
			return nEntry;
		}
		// [size(4)][sorted len(4)][next blk][prev blk][......]
		static NodeEntry loadUnsorted(ByteBuffer buf, KVEntryImp entry) {
			buf.limit(buf.getInt()); // Set size to limit
			buf.position(buf.getInt()); // Set position to unsorted start
			NodeEntry nEntry = new NodeEntry();
			loadEntries(buf, nEntry, entry);
			buf.clear();
			return nEntry;
		}
		// common method to load entries
	    static void loadEntries(ByteBuffer bBuffer, NodeEntry nEntry, KVEntryImp entry) {
			List<KVEntry> list = new LinkedList<>();
			KVEntryImp tmp = null;
			while ((tmp = KVEntryImp.dSerialize(bBuffer)) != null)
				list.add(tmp);
			if (entry != null)
				list.add(entry);
			nEntry.entries = list.toArray(new KVEntryImp[0]);
		}	    
	}
	
	/**
	 * KVEntry implementation
	 * @author 
	 *
	 */
	static final class KVEntryImp implements KVEntry {
		/** meta size require to serialise KV Entry */ 
		static final int KV_META_SZ = 21;
		/** Byte array key */
		byte[] key = null; 
		/** If the entry was deleted */
		boolean dMarker = false; 
		/** Value could be Node/Long/byte[] */
		Object value = null;
		/** total size of the entry */
		int size = 0; 
		/** Time stamp  */
		long ts = -1;
		
		KVEntryImp() {}
		KVEntryImp(byte[] key, byte[] val, boolean dMark) { this(key, val); this.dMarker = dMark; }
		KVEntryImp(byte[] key, Object val, int size) { init(key, val, false, size, System.nanoTime()); }
		KVEntryImp(byte[] key, Object val, boolean dMark, int size, long ts) { init(key, val, dMark, size, ts); }
		
		/**
		 * Create a Key value entry and perform validation		
		 * @param key
		 * @param val
		 */
		private KVEntryImp(byte[] key, byte[] val) { 
			checkKey(key);
			checkValue(val);			
			init(key, val, false, 0, System.nanoTime());
			size = sSize();
		}
		
		/**
		 * Check if key is null or empty and size limit
		 * @param key
		 */
		private void checkKey(byte[] key) {
			if (key == null || key.length == 0)
				throw new NullPointerException("Key can not be null or empty");
			if (key.length > MAX_KEY_SIZE)
				throw new RuntimeException("Key length can not be greater than " + MAX_KEY_SIZE + " bytes");
		}
		/**
		 * check if value is null or empty
		 * @param val
		 */
	    private void checkValue(byte[] val) {
	    	if (val != null && val.length == 0)
	    		throw new RuntimeException("Value can not be empty");    		
	    }
	    /**
	     * Common method for constructors. initialise all relevant fields
	     * @param key
	     * @param val
	     * @param dMark
	     * @param size
	     * @param ts
	     */
		void init(byte[] key, Object val, boolean dMark, int size, long ts) {
			this.dMarker = dMark;
			this.key = key;
			this.value = val;
			this.size = size;
			this.ts = ts;
		}
		
		@Override
		public String toString() {
			String keyStr = key != null ? Arrays.toString(key) : "null";
			StringBuilder sbdlr = new StringBuilder(keyStr);
			String valStr = "null";
			if (value != null) {
				if (value instanceof byte[]) 
					valStr = ((byte[]) value).length + "";
				else if (value instanceof Node) {
					NodeEntry nEntry = ((Node) value).getNodeEntry();
					valStr = nEntry.blkNo + "BN";
				} 
				else {
					long bN = (long) value;
					int fn = (int) (bN >> 32);
					int pos = (int) (bN);
					valStr = fn + ":" + pos;
				}
			}
			sbdlr.append("," + valStr);
			sbdlr.append("," + size);
			sbdlr.append("," + ts);
			sbdlr.append("," + dMarker);
			return sbdlr.toString();
		}
		// [tot len(4)][TS(8)][size (4)][dMarker(1)][key len(4)][Key....][Value...]
		int sSize() {
			int sz = KV_META_SZ;
			sz += key != null ? key.length : 0;
			if (value == null) return sz;
			else if (value instanceof byte[])
				return sz += ((byte[]) value).length + 1;
			else
				return sz += 9;
		}
		/** [tot len(4)][TS(8)][size (4)][dMarker(1)][key len(4)][Key....][Value...] */
		ByteBuffer serialize(ByteBuffer bBuffer) {
			int sz = sSize();
			if (bBuffer == null)
				bBuffer = ByteBuffer.allocate(sz);
			// Set total LEN of the entry
			bBuffer.putInt(sz);
			// Set Time stamp
			bBuffer.putLong(ts);
			// size of the entry
			bBuffer.putInt(size);
			// Set deletion marker
			bBuffer.put(dMarker ? (byte) 1 : 0);
			// copy key bytes if not null
			if (key == null) 
				bBuffer.putInt(0);				
			else {
				bBuffer.putInt(key.length);
				bBuffer.put(key);
			}
			// Check value
			if (value != null) {
				if (value instanceof Long)
					bBuffer.put((byte) 1).putLong((long) value);
				else
					bBuffer.put((byte) 0).put(((byte[]) value));
			}
			return bBuffer;
		}		
		/** [tot len(4)][TS(8)][size (4)][dMarker(1)][key len(4)][Key....][Value...] */
		static KVEntryImp dSerialize(ByteBuffer bBuffer) {
			if (!bBuffer.hasRemaining()) 
				return null;
			KVEntryImp entry = new KVEntryImp();
			// total sizes
			int totSize = bBuffer.getInt();
			// Set time stamp
			entry.ts = bBuffer.getLong();
			// Size of the entry, for LOB it will bigger than key/value
			entry.size = bBuffer.getInt(); 
			// get delete marker
			entry.dMarker = bBuffer.get() != 0 ? true : false;
			// key size
			int kSz = bBuffer.getInt();
			// Get key bytes		
			if ( kSz > 0 ) {
				entry.key = new byte[kSz]; 
				bBuffer.get(entry.key); 				
			}
			// Get value bytes
			int valSz = totSize - ( kSz  + KV_META_SZ );
			if ( valSz > 0 ) {
				if ( bBuffer.get() > 0 )
					entry.value = bBuffer.getLong();
				else {
					entry.value = new byte[valSz - 1];
					bBuffer.get((byte[])entry.value);					
				}
			}		
			return entry;
		}
		/**
		 * Get a copy of entry
		 * @return
		 */
		KVEntryImp getACopy() {
			KVEntryImp newEntry = new KVEntryImp();
			newEntry.key = copyKey();
			newEntry.value = copyValue();
			newEntry.size = size;
			newEntry.ts = ts;
			newEntry.dMarker = dMarker;
			return newEntry;
		}
		/**
		 * Copy key
		 * @return
		 */
		private byte[] copyKey() {
			if (key == null) return null;
			byte[] keyNew = new byte[key.length];;
			System.arraycopy(key, 0, keyNew, 0, key.length);
			return keyNew;
		}
		/**
		 * Copy value
		 * @return
		 */
		private Object copyValue() {
			if (value == null)
				return null;
			if(value instanceof Long)
				return value;
			byte[] val = new byte[((byte[]) value).length];
			System.arraycopy(value, 0, val, 0, val.length);
			return val;
		}
		@Override
		public byte[] getKey() {
			return key;
		}
		@Override
		public byte[] getValue() {
			return (byte[]) value;
		}
		
		@Override
		public int hashCode() {
			return Arrays.hashCode(key);
		}
		@Override
		public boolean equals(Object other) {
			return Arrays.equals(key, ((KVEntryImp) other).key);
		}
		@Override
		public boolean isDeleted() { return dMarker; }
		@Override
		public int size() {	return size; }
		@Override
		public long timeStamp() { return ts; }
	}

	/**
	 * Iterator implementation
	 * 
	 * @author pan.prasanta@gmail.com
	 *
	 */
	private final class IteratorImp implements KVIterator {
		/** Hold currently loaded blocks */
		NodeEntry crnt = null;
		// Current and last entry
		KVEntryImp start, end, entry;
		// Close indicator
		boolean close = false;
		// order of iteration
		boolean rev = false;
		// current entry position
		int pos = 0;		

		private IteratorImp(byte[] from, byte[] to, boolean rev) throws Exception {
			// reverse indicator
			this.rev = rev;
			// create end key if not null
			if (to != null)
				end = new KVEntryImp(to, null , true);
			// create start key if not null
			if (from != null) 
				start = new KVEntryImp(from, null, true);
			/**
			 * Verify key range
			 */
			if (start != null && end != null) {
				int status = eComp.compare(start, end);
				if ((rev && status < 0) || (!rev && status > 0)) {
					throw new RuntimeException("start > end");	
				}
			}						
			/**
			 * If reverse iterator and no start key provided than start from the end
			 */
			if (rev && start == null) {
				// load last block
				crnt = loadLastBlock();
				pos = crnt.Len() - 1;
													
			}
			/**
			 * If forward iterator and no start key was provided than start from the beginning
			 */
			else if (!rev && start == null) {
				crnt = loadAndMerge(0L); // load first block
				pos = 0;
				
			}	
			/**
			 * if we got to start from somewhere 
			 */
			else if (start != null) {
				RootNode lNode = rNode;
				findEntry(lNode.root, lNode.ht, start);
				crnt = (NodeEntry) start.value;
				pos = start.size;
			}
			// no data
			if (crnt.Len() == 0) {
				close();
				return;
			}
			// calculate position
			pos = rev ? pos < 0 ? abs(pos) - 2 : pos : pos < 0 ? abs(pos) - 1 : pos;				
		}
		/**
		 * Load block from disk
		 * @return
		 * @throws Exception
		 */
		private boolean loadBlk() throws Exception {
			// if reveres iterator
			if (rev) {
				// if end of iteration
				if ((crnt = loadPrevBlk(crnt.blkNo)) == null) {
					close();
					return false;
				}
				pos = crnt.Len() - 1;
			} else {
				// at the end of iteration
				long bNo;
				if ((bNo = crnt.nextBlk()) == -1L) {
					close();
					return false;
				}
				// load next block
				crnt = loadAndMerge(bNo);
				pos = 0;
			}
			// if everything is all right
			return true;
		}
		
		@Override
		public boolean hasNext() throws Exception {			
			return !close ? rev ? revNext() : fwdNext() : false;		
		}
		
		@Override
		public KVEntry next() throws Exception {
			if (!close && entry != null)
				return entry;
			// 
			throw new NoSuchElementException();	
		}
		
		@Override
		public void close() {
			crnt = null;
			close = true;	
			start = end = entry = null;			
		}
		
		/**
		 * Iterate in forward or natural order
		 * @return
		 * @throws Exception
		 */
		private boolean fwdNext() throws Exception {
			// loop until we get a valid entry
			for (;;) {
				if (pos == crnt.Len() && !loadBlk()) // load next block
					return false;
				entry = crnt.entries[pos++];
				// ignore deleted entries
				if (!entry.dMarker && (entry = loadLob(entry)) != null)
					break;
			}
			// if we reached a dead end
			if (end != null && eComp.compare(entry, end) >= 0)
				return false;
			// if everything all right
			return true;
		}
		
		/**
		 * Iterate in reverse order
		 * @return
		 * @throws Exception
		 */
		private boolean revNext() throws Exception {
			// loop until we get a valid entry
			for (;;) {
				if (pos == -1 && !loadBlk()) // load next block
					return false;
				entry = crnt.entries[pos--];
				// ignore deleted entries
				if (!entry.dMarker && (entry = loadLob(entry)) != null)
					break;
			}
			// check if we reached a dead end already
			if (end != null && eComp.compare(entry, end) <= 0)
				return false;
			// if everything all right
			return true;
		}
		
	}

}
