package org.pp.storagengine.api;

public interface KVEntry {
	/**
	 * Return the byte array key
	 * @return
	 */
   public byte[] getKey();
   /**
    * Return the byte array value
    * @return
    */
   public byte[] getValue();
   /**
    * Entry size in bytes
    * @return
    */
   public int size();
   /**
    * Return the time stamp in nano seconds when the entry was created
    * @return
    */
   public long timeStamp();
   /**
    * Return true if the entry was deleted
    * @return
    */
   public boolean isDeleted();
   
}
