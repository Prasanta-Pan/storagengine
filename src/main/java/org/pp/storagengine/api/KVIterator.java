/**
 * 
 */
package org.pp.storagengine.api;

/**
 * @author prasantapannew
 *
 */
public interface KVIterator {
  public boolean hasNext() throws Exception;
  public KVEntry next() throws Exception ;
  public void close();
}
