package org.pp.storagengine.api;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pp.storagengine.api.imp.ConcurrentTest;

public class JUnitConcurrentTest {
	@BeforeClass // setup test
	public static void setUp() throws Exception {
		ConcurrentTest.setup();
	}

	@AfterClass // destroy engine
	public static void destroy() throws Exception {
		ConcurrentTest.cleanup();
	}
	
	@Test() // First test all parameter correctness
	public void test1() throws Exception { 
		ConcurrentTest.execute();
	}
}
