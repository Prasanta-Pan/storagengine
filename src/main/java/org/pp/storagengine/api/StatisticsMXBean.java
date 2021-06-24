package org.pp.storagengine.api;

public interface StatisticsMXBean {
	// All runtime statistics
	/**
	 * Height of the Tree
	 * @return
	 */
    public int getHt();
    /**
     * Total number nodes based on height parameter
     * @param ht
     * @return
     */
    public int getNmOfNode(int ht);
    /**
     * Number of active records in database
     * @return
     */
    public long getNumOfActRecs();
    /**
     * Number of entries deleted so far
     * @return
     */
    public long getNumOfDelRecs();
    /**
     * Number of data files so far
     * @return
     */
    public int getNumOfDataFiles();
    /**
     * Number of branch entry
     * @return
     */
    public int getNumOfBranchEntry();
    /**
     * Number of disk load
     * @return
     */
    public long getNumOfLoad();
    /**
     * number of database sync
     * @return
     */
    public long getNumOfSync();
    /**
     * Sync after this number of write
     * @return
     */
    public int getMaxBlkSync();
    /**
     * Approximate size of database
     * @return
     */
    public long getApprxSize();
    /**
     * Last sync time in nano seconds
     * @return
     */
    public long getLastSyncTime();
    /**
     * Maximum time for sync in milliseconds
     * @return
     */
    public long getMaxSyncTime();
       
    // All parameters
    /**
     * Data block size
     * @return
     */
    public int getDataBlockSize() ;
    /**
     * Max data file size
     * @return
     */
	public int getMaxDataFileSize() ;
	/**
	 *  Max number of blocks per data file
	 * @return
	 */
	public int getMaxBlockPerFile() ;
	/**
	 * Database directory location
	 * @return
	 */
	public String getRootDir() ;
	/**
	 * Max LOB size defined
	 * @return
	 */
	public int getMaxLobSize() ;	
}
