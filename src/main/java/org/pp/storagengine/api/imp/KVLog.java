package org.pp.storagengine.api.imp;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KVLog {
	private static DateTimeFormatter formt = null;
	// Initialize
	static {
		formt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");			
	}
	public synchronized static void error(Throwable e) { 
		System.err.println(logStr("")) ; 
		e.printStackTrace(System.err); 
	}
	public static void println(Object obj) { System.out.println(logStr(obj)); }
	public static void print(Object obj) { System.out.print(logStr(obj)); }
	
	// Create log String
	static String logStr(Object obj) {
		String tName = "[" + Thread.currentThread().getName() + "]";
    	tName = tName + "[" + LocalDateTime.now().format(formt) + "]:" + obj;
    	return tName;
	}
	
}
