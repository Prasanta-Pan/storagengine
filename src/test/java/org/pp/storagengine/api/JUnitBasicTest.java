package org.pp.storagengine.api;

import static org.pp.storagengine.api.imp.BasicTest.commonTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pp.storagengine.api.imp.BasicTest;

public class JUnitBasicTest {
	   
	@BeforeClass // setup test
	public static void setUp() throws Exception {
		BasicTest.setUp();
	}

	@AfterClass // destroy engine
	public static void destroy() throws Exception {
		BasicTest.destroy();
	}
	
	@Test() // First test all parameter correctness
	public void test1() throws Exception { 
		commonTest();
	}
	
	@Test() // First test all parameter correctness
	public void test2() throws Exception {
		commonTest();
	}

}
