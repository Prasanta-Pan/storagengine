package org.pp.storagengine.api.imp;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.pp.storagengine.api.StatisticsMXBean;

class Statistics implements StatisticsMXBean {
	ConcurrentMap<Integer,Integer> htMap = new ConcurrentHashMap<>();
	private SysContext ctx = null;
	volatile int 	ht 					= 0;
	volatile long 	numOfActRecs 		= 0;
	volatile long 	numOfDelRecs 		= 0;
	volatile int 	numOfDataFiles 		= 1;
	volatile int 	numOfBranchEntry 	= 1;
	volatile long 	numOfLoad 			= 0;
	volatile long 	apprxSize 			= 0;
	volatile long 	lastSyncTime 		= 0;
	volatile long 	maxSyncTime 		= 0;
	volatile long	numOfSync 			= 0;
			
				
	@Override
	public int getHt() { return ht; }

	@Override
	public int getNmOfNode(int ht) { return htMap.get(ht); }

	@Override
	public long getNumOfActRecs() { return numOfActRecs; }

	@Override
	public long getNumOfDelRecs() { return numOfDelRecs; }

	@Override
	public int getNumOfDataFiles() { return numOfDataFiles; }

	@Override
	public int getNumOfBranchEntry() { return numOfBranchEntry; }

	@Override
	public long getApprxSize() { return apprxSize; }

	@Override
	public long getLastSyncTime() { return lastSyncTime; }
	
	@Override
	public long getNumOfSync() { return numOfSync; }

	@Override
	public int getDataBlockSize() { return ctx.getBlockSize(); }

	@Override
	public int getMaxDataFileSize() { return ctx.getMFileSize(); }

	@Override
	public int getMaxBlockPerFile() { return ctx.getMBlockFile(); }
	
	public int getMaxBlkSync() { return ctx.getMaxBlkSync(); } 

	@Override
	public String getRootDir() { return ctx.getRootDir(); }

	@Override
	public int getMaxLobSize() { return ctx.getMaxLobSize(); }

	@Override
	public long getNumOfLoad() { return numOfLoad; }	
	
	@Override
	public long getMaxSyncTime() { return maxSyncTime;  }
		
	private ObjectName objectName = null;	
	
	private Statistics() { htMap.put(ht, 0); htMap.put(-1, 0); }	
	static Statistics register(SysContext ctx) throws Exception {
		Statistics mBean = new Statistics(); mBean.ctx = ctx;
		ObjectName objectName = new ObjectName("org.pp.storageengine.api.imp:type=Statistics");
	    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
	    server.registerMBean(mBean, objectName);
	    mBean.objectName = objectName;	    
		return mBean;
	}
	void unregister() throws Exception {
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		server.unregisterMBean(objectName);
	}	
}
