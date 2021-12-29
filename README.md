# storagengine
**storagengine is a fast key-value storage library written in Java that provides an ordered mapping from byte[] keys to byte[] values.**

# Features

  * Keys and values are arbitrary byte arrays.
  * Data is stored sorted by key.
  * Callers can provide a custom comparison function to override the sort order.
  * The basic operations are `put(key,value)`, `get(key)`, `delete(key)`.
  * Forward and backward iteration is supported over the data.
  * Large value's (also called LOB) are supported
  * Internal state and data structured can be monitored by JMX Mbeans in runtime
  * This tiny library has been implemented using B+ Tree
  * Highly portable and can be embedded easly in any java based application.
  * Highly concurrent, high performance key value engine.
  
 
# Getting the Source

```bash
git clone --recurse-submodules https://github.com/Prasanta-Pan/storagengine.git
```

# Building

First, please ensure [Maven](https://maven.apache.org/) already installed in local machine or buildbox.
After downloading the source (as mentioned above), switch to source folder and issue the following command

```bash
mvn clean package
```
# Using the API's
Storage engine library provide a persistent key value store. The keys are ordered according to the 
user provided custom comparator function. Below sections will walk you through how to create/open a key-value
database, use the API to manipulate/access Key-Value and iterators.

## Opening/Creating Key-Value Database
Below code sample shows us how to create a brand new database or open an existing one.

First create a byte array comparator...

```
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
```
Than create a sample config properties....


```
		/**
		 * User can either use standard java system properties or
		 * can create their own properties if they want to
		 */
		Properties props = System.getProperties();
		/**
		 * This property indicate data block or page size.
		 * The value must be always power of 2 starting from 4KB.
		 */
		props.setProperty("dataPageSize", "4KB");
		/**
		 * This property indicate maximum size of particular
		 * key value pair (Also called LOB)
		 */
		props.setProperty("maxLobSize", "1MB");
		/**
		 * Key-Values are organised (sorted) in files.
		 * This property indicate the engine, how big a database file could be.
		 * Obviously, for big database we will have several database files
		 * in database directory.
		 */
		props.setProperty("dataFileSize", "32MB");
		/**
		 * By default not every put operation followed by disk sync command.
		 * This property indicate after exactly how many write operations (put or delete)
		 * sync command will be issued in order to ensure durability of data.
		 */
		props.setProperty("maxBlkSync", "128");
```

Note that the above properties are default properties, means if users choose not set any properties at all
they can simply pass an empty Property object to the engine as shown below.

```
		/**
		 * User can either use standard java system properties or
		 * can create their own properties if they want it
		 */
		Properties props = new Properties();		
```

Now we are ready to create (or open an existing key value database)

```
import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.imp.KVEngineImp;
	.....
	....
         /**
		 * Our database is organised in Directory. 
		 * All database files and related config files are stored in a directory.
		 * dbPath indicate where we want to create our database or open it (if any).
		 */
		String dbPath = /path/to/db/test
		// now open or create KV Store
		KVEngine db = new KVEngineImp(dbPath, props, myComp);
```

Note that, the KVEngine instance (db) is thread safe, means all the KVEngine API's can be invoked concurrently from multiple threads without any issue. But creating a KVEngine instance is not thread safe.

## Using put(), get() and delete()

The KVEngine provides Put, delete, and get methods to modify/query KV database.
Few examples below.

```
import org.pp.storagengine.api.KVEngine;
	.....
	....
          /**
		 * Some series of Put operations
		 */
		byte[] key = "key1".getBytes();
		byte[] value = "value1".getBytes();
		// persist key value pair
		db.put(key, value);
		// some more
		byte[] key2 = "key1".getBytes();
		byte[] value2 = "value1".getBytes();
		// persist key value pair
		db.put(key2, value2);
		 /**
		 * By default disk sync is issued after 128 writes.
		 * But user can always invoke manual sync 
		 */
		db.sync();
		 /**
		 * A delete operation
		 */
		 db.delete(key2);
		 /**
		 * A get operation
		 */
		 value = db.get(key);
		 /**
		 * Close the DB when we are done
		 */
		 db.close();
```

## Using iterators

The following code block demonstrates the use of iterators.

```
import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;
import org.pp.storagengine.api.KVIterator;
	.....
	....
         /**
		 * Opening a basic iterator.
		 * Which will iterate over all Key Values in database
		 */
		KVIterator itr = db.iterator();
		while (itr.hasNext()) {
		    /**
		     *  Get next KVEntry
		     */
		     KVEntry e = itr.next();
		     /**
		     *  Print key value or do something useful with it
		     */
		     // assuming key and value both are Strings
		     String key = new String(e.getKey());
		     String value = new String(e.getValue());
		     // print now
		     System.out.println("Key: " + key + ", value: " + value);
		}
		
		/**
		 * A reverse iterator can also be opened.
		 * Which will iterate over all Key Values in database in descending order 
		 */
		 itr = db.iterator(true);
		 ....
		  /**
		 * Opening a iterator which starts from the specific key.
		 * It doesn't matter if the specified key exist in database or not.
		 * In that case the iterator will starts with the next bigger key in existence.
		 */
		  byte[] key = "key111".getBytes();
		  itr = db.iterator(key);
		  ......
	     /**
		 * Iterating over a specific key range.
		 */
		 
		  byte[] from = "key111".getBytes();
		  byte[] to = "key200".getBytes();
		  itr = db.iterator(from, to);
		  ....
		 /**
		 * Close the DB when we are done
		 */
		 db.close();
```

## Fetching first key-value entry in the store/database

Below code block demonstrates how to fetch first entry in the KV database.

```
import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;

	.......
		.......
		// Get the first KV entry of the store
		KVEntry entry = db.firstEntry();
          // print it
          System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue())
```

## Fetching last key-value entry in the store/database

Below code block demonstrates how to fetch last entry in the KV database.

```
import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;

	.......
		.......
		// Get the last KV entry of the store
		KVEntry entry = db.lastEntry();
          // print it
          System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue())
```

## Fetching a greater key-value entry (if exist) of a specified key in the store/database

Below code block demonstrates how to fetch a greater key-value entry (if exist) of a specific key.

```
import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;

	.......
		.......
		// check if a greater key exist
		byte[] key = "key11".getBytes();
		KVEntry entry = db.nextEntry();
          // print it
          System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue())
```

## Fetching a lower key-value entry (if exist) of a specified key in the store/database

Below code block demonstrates how to fetch a lower key-value entry (if exist) of a specific key.

```
import org.pp.storagengine.api.KVEngine;
import org.pp.storagengine.api.KVEntry;

	.......
		.......
		// check if a lower key exist
		byte[] key = "key11".getBytes();
		KVEntry entry = db.prevEntry();
          // print it
          System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue())
```
## More code samples

More code samples and usage examples including but limited to concurrency can be found in Junit test package.

