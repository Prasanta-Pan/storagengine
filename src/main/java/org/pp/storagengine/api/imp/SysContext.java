package org.pp.storagengine.api.imp;

import static org.pp.storagengine.api.imp.MyValidator.propToObj;
import static org.pp.storagengine.api.imp.MyValidator.toProperties;
import static org.pp.storagengine.api.imp.MyValidator.validateProps;
import static org.pp.storagengine.api.imp.Util.KB;
import static org.pp.storagengine.api.imp.Util.MB;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;

import org.pp.storagengine.api.Validator;

class SysContext {
	/** File name where system parameters will be saved */
	private static final String sInfoFile = File.separator + ".info";
	/** Root directory path and name */
	private String ROOT_DIR = "";
	/** Data page size (Should be aligned with FS disk block size) */
	@Validator(min = 4 * KB, powof2 = true)
	private int dataPageSize = 4 * 1024; 
	/** Data file size in MB, default 32 MB */
	@Validator(min = MB, powof2 = true)
	private int dataFileSize = 32 * MB; 
	/** Maximum data block size per data file */
	private int mBlockFile;
	/** Maximum LOB size, default is 10MB */
	@Validator()
	private int maxLobSize = 10 * MB;	
	/** Sync after specified number of block update */
	@Validator(powof2 = true, max = 512)
	private int maxBlkSync = 128;
	/** If compression enabled */
	@Validator()
	private boolean compression = false;
	/** If checksum enabled */
	@Validator()
	private boolean checksum = false;
	
	int getBlockSize() { return dataPageSize; }
	int getMFileSize() { return dataFileSize; }
	int getMBlockFile() { return mBlockFile; }
	String getRootDir() { return ROOT_DIR; }
    int getMaxLobSize() { return maxLobSize; }	
	int getMaxBlkSync() { return maxBlkSync; }
	boolean isCompression() { return compression; }
	boolean isChecksum() { return checksum; 	}
	
	SysContext(String root,Properties options) throws Exception { 
		if (root == null || "".equals(root.trim()))
			throw new RuntimeException("DB Directory is null or empty!");
		/** Create if directory do not exist */
		File rootDir = new File(root);
		if (!rootDir.exists() && !rootDir.mkdir())
			throw new RuntimeException("Couldn't create directory");
		this.ROOT_DIR = root; 
		// context load or save
		if (!loadConfigs()) { validateProps(this,options); saveConfigs(); }
		// Maximum block per file
		mBlockFile = dataFileSize / dataPageSize;
		// if max block not provided by user	
		if (checksum) {
			throw new RuntimeException("Checksum calculation not implemented yet");
		}
	}	
	// Save context information
	private void saveConfigs() throws Exception {
		Properties options = toProperties(this);
		File tFile = new File(ROOT_DIR + sInfoFile);
		FileWriter fwr = new FileWriter(tFile);
		options.store(fwr, null);
		fwr.flush(); 
		fwr.close();		
	}
	// Load configurations
	private boolean loadConfigs() throws Exception {
		File sFile = new File(ROOT_DIR + sInfoFile);
		if (!sFile.exists()) return false;
		FileReader fRdr = new FileReader(sFile);
		Properties props = new Properties(); 
		props.load(fRdr); 
		propToObj(this, props);
		return true;		
	}	
}
