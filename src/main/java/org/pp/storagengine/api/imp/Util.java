package org.pp.storagengine.api.imp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.pp.storagengine.api.Validator;

public class Util {
	// Size definitions
	public static final int MB = 1024 * 1024;
	public static final long GB = 1024 * (long) MB;
	public static final int KB = 1024;	 
	public static final int mSec = 1000 * 1000;
	
	// for displaying size
	public static String calSize(long sz) {
		if (sz < 0) return "" + sz;
		else if (sz < MB) return sz / 1024 + "KB";
		else if (sz < GB) return sz / MB + "MB";
		else return sz / GB + "GB";
	}
	public static byte[] copyBytes(byte[] data) {
		byte[] newDate = new byte[data.length];
		System.arraycopy(data, 0, newDate, 0, data.length);
		return newDate;
	}
	public static byte[] extrcBytes(byte[] src, int offset, int len) {
		byte[] data = new byte[len];
		System.arraycopy(src, offset, data, 0, len);
		return data;
	}	
	// Atomic rename
	public static void atoRename(String src, String dest) throws IOException {
		Path nsrc = Paths.get(src);
		Path ndest = Paths.get(dest);
		Files.move(nsrc, ndest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
	}	
	// Common method for both LONG / INT
	public static final long validateNum(long val, Validator validator, String key) {
		if (!validator.negtv() && val < 0)
			runExcp("Negative value is not allowed for the field '" + key + "'");
		if (validator.min() > 0 && val < validator.min())
			runExcp("Minimum value for the field '" + key + "' is " + validator.min());
		if (validator.max() > 0 && val > validator.max())
			runExcp("Maximum value for the field '" + key + "' is " + validator.max());
		// Check power of 2
		if (!validator.negtv() && validator.powof2() && !isPowOf2(val)) {
			if (validator.min() < 0)
				runExcp("The value must be power of 2 for the field '" + key + "'");
			else
				runExcp("The value must be power of 2 for the field '" + key + "' min value: " + validator.min());
		}
		return val;
	}
	// Extract number from String value e.g. 4KB, 2MB, 10GB
	public static final long extrNum(String val, String fldName) {
		int index = 0;
		val = val.toUpperCase();
		if ((index = val.lastIndexOf('K')) > 0)
			return Integer.parseInt(val.substring(0, index)) * 1024;
		else if ((index = val.lastIndexOf('M')) > 0)
			return Integer.parseInt(val.substring(0, index)) * MB;
		else if ((index = val.lastIndexOf('G')) > 0)
			return Long.parseLong(val.substring(0, index)) * GB;
		else
			return Long.parseLong(val);
	}
	//
	public static final void runExcp(String msg) {
		throw new RuntimeException(msg);
	}
	// Power of 2 check
	public static final boolean isPowOf2(long numb) {
		return numb > 0 && ((numb & (numb - 1)) == 0);
	}
	
	@SuppressWarnings("static-access")
	public static void wThread(Thread t) throws Exception {
		while(t != null && t.isAlive())
			Thread.currentThread().sleep(50);
	}	
}
