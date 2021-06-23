package org.pp.storagengine.api.imp;

public class MainTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String t = System.getProperty("test");
		// in case no value supplied by user
		t = (t == null) ? "" : t;
		switch (t) {
			case "conc" 		:
			case "concurrent" 	:
				ConcurrentTest.main(null);
				break;
			case "basic" 		: 
			default 			:
				BasicTest.main(null);				
		}
	}

}
