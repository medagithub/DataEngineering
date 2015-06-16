package com.insightdataeng;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public class FileReaderThread2 extends Thread{
	private static Logger logger = Logger.getLogger(FileReaderThread.class);
	public static RunningMedian rm = null;
	private String fileName = null;
	private int index = -1;
	private List<Integer> wordCountsByLine = new ArrayList<Integer>();	

	
	public FileReaderThread2(RunningMedian rm, String fileName, int index) {
		this.fileName = fileName;
		this.rm = rm;
		this.index = index;
	}
	
	public void run() {
		 logger.debug("FileName : " + this.fileName +" Reader Thread -  "  + Thread.currentThread().getName());
		 File file = new File(fileName);
		 if (file == null || !file.exists() || file.isDirectory()) {
			 return;
		 }
		 long startTime = System.currentTimeMillis();
		 BufferedReader rd = null;
		 try {
			 rd = new BufferedReader(new FileReader(file));
			 String line = null;
			 StringTokenizer tokens = null;
			 while ((line = rd.readLine()) != null) {
				tokens = new StringTokenizer(line, " ");
				wordCountsByLine.add(tokens.countTokens()); 
			 }
			 tokens = null;
			
		 } catch (Exception e) {
			 logger.error("Exception while reading file - " + fileName + "  Reader Thread - " + Thread.currentThread().getName());
			 logger.error(e.getMessage(), e);
		 } finally {
			 try {
				 if (rd != null) rd.close();
			 } catch(Exception e) { }	
			 rm.getWordCountsPerFileMap().put(index, wordCountsByLine);
		 }
		 logger.debug("Reader Thread - " + Thread.currentThread().getName() + "  Done reading file:" + fileName + "  Number of lines = " + rm.getWordCountsPerFileMap().get(index).size() + " rm.getWordCountsPerFileMap().size() = " + rm.getWordCountsPerFileMap().size() + " Time taken = " + (System.currentTimeMillis() - startTime));
	}
}
