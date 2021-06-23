package org.pp.storagengine.api;

class ReadWriteLockImp extends ReadWriteLock {

	@Override
	public void unlock() {
		// get the key locker object
		ReadWriteLock kl = map.get(lk.key);
		if (kl != null) {
			synchronized (kl) {
				if (kl.lType == READ) {
					kl.rCount--;
					if (kl.rCount == 0) {
						kl.wait = false;
						if (kl.wLock) {
							kl.notifyAll();
							return;
						}						
						map.remove(lk.key);
					}
				} else { // must be writer unlocking key
					map.remove(lk.key);
					kl.wait = false;
					kl.notifyAll();
				}
				// finally remove the mapping
				map.remove(lk);
			}
		}		
	}
}
