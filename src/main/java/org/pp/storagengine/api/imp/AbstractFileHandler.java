package org.pp.storagengine.api.imp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

abstract class AbstractFileHandler {
	/** Data file and related staff */
	static final String dfPrefix = File.separator + "df";
	static final String dfPostfix = ".db";

	/** Read only system context available to all component */
	protected SysContext ctx;
	protected Statistics stat;

	/**
	 * Read LOB from disk
	 * 
	 * @param bNo
	 * @param size
	 * @return
	 * @throws Exception
	 */
	protected byte[] readBlk(long bNo, int size) throws Exception {
		byte[] data = new byte[size];
		int fileNo = (int) (bNo >> 32);
		String fName = getFileName(fileNo, dfPrefix, dfPostfix);
		try (RandomAccessFile rf = new RandomAccessFile(fName, "r")) {
			rf.seek(ctx.getBlockSize() * (int) bNo);
			rf.read(data);
			return data;
		}
	}

	/**
	 * A wrapper version of readBlk
	 * 
	 * @param blkId
	 * @return
	 * @throws Exception
	 */
	protected ByteBuffer readBlk(long blkId) throws Exception {
		return ByteBuffer.wrap(readBlk(blkId, ctx.getBlockSize()));
	}

	/** create file name */
	protected String getFileName(int fNo, String pFix, String poFix) {
		StringBuilder sbldr = new StringBuilder(ctx.getRootDir());
		return sbldr.append(pFix).append(fNo).append(poFix).toString();
	}

	/** Get the latest file */
	protected Object[] latestFile(String pFix, String poFix) throws Exception {
		int fN = 0, tmp = 0;
		pFix = pFix.substring(1);
		final File[] files = new File(ctx.getRootDir()).listFiles();
		String fName = null;
		for (File f : files) {
			fName = f.getName();
			if (fName.startsWith(pFix) && fName.endsWith(poFix)) {
				tmp = fNo(pFix, fName);
				if (tmp > fN)
					fN = tmp;
			}
		}
		File f = new File(getFileName(fN, File.separator + pFix, poFix));
		return new Object[] { f, fN };
	}

	private int fNo(String pFix, String fName) {
		String[] split = fName.split("\\.");
		return Integer.parseInt(split[0].substring(pFix.length()));
	}

	/** get file list sorted chronological order */
	protected List<File> allFiles(String preFix, String postFix) throws Exception {
		preFix = preFix.substring(1);
		final File[] files = new File(ctx.getRootDir()).listFiles();
		List<File> lFiles = new ArrayList<>();
		String fName = null;
		for (File f : files) {
			fName = f.getName();
			if (fName.startsWith(preFix) && fName.endsWith(postFix))
				lFiles.add(f);
		}
		final String pf = preFix;
		/** Sort in chronological order */
		Collections.sort(lFiles, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				// TODO Auto-generated method stub
				int l = fNo(pf, o1.getName());
				int r = fNo(pf, o2.getName());
				if (l != r)
					return l - r;
				return 0;
			}
		});
		return lFiles;
	}

	/** sync and close buffer and file */
	protected void close(FileOutputStream fOut, BufferedOutputStream bOut) throws Exception {
		// Flash and close log file/buffer
		sync(fOut, bOut);
		fOut.close();
	}

	/** sync and close buffer and file */
	protected void close(RandomAccessFile rF) throws Exception {
		// Sync and close file
		sync(rF);
		rF.close();
	}

	/** sync buffer and file */
	protected void sync(FileOutputStream fOut, BufferedOutputStream bOut) throws Exception {
		flush(fOut, bOut);
		sync(fOut.getChannel());
	}

	protected void flush(FileOutputStream fOut, BufferedOutputStream bOut) throws Exception {
		if (bOut != null)
			bOut.flush();
		fOut.flush();
	}

	protected void sync(RandomAccessFile rF) throws Exception {
		sync(rF.getChannel());
	}

	protected void sync(FileChannel fc) throws Exception {
		fc.force(true);
	}

}
