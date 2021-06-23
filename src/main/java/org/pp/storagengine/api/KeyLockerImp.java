package org.pp.storagengine.api;

class KeyLockerImp extends KeyLockerNew {

	@Override
	public void unlock() {
		// Remove the key first
		KeyLockerNew kl = map.remove(lk.key);
		if (kl != null) {
			synchronized (kl) {
				// Make waiting false to ensure reader never wait forever
				kl.wait = false;
				// Signal others that I am done
				kl.notifyAll();
				// remove other mapping as well;
				map.remove(lk);
			}
		}
	}

}
