package org.pp.storagengine.api.imp;

public class TryAnsiEsCode {

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
        for (int i = 0; i < 10; i++) {
        	System.out.print("Line 1" + i + "\n");
        	System.out.print("Line 2" + i + "\n");
        	System.out.print("Line 3" + i + "\n");
        	System.out.print("\033[3A");
        	Thread.currentThread().sleep(1000);        	
        }
	}

}
