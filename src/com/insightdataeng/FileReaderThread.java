package com.insightdataeng;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;

import org.apache.log4j.Logger;

public class FileReaderThread extends Thread {
	private static Logger logger = Logger.getLogger(FileReaderThread.class);
	public static WordCount wc = null;
	private String fileName = null;
	
	public FileReaderThread(WordCount wc, String fileName) {
		this.fileName = fileName;
		this.wc = wc;
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
			 while ((line = rd.readLine()) != null) {
				 wc.getLines().add(line);
			 }
			
		 } catch (Exception e) {
			 logger.error("Exception while reading file - " + fileName + "  Reader Thread - " + Thread.currentThread().getName());
			 logger.error(e.getMessage(), e);
		 } finally {
			 try {
				 if (rd != null) rd.close();
			 } catch(Exception e) { }
			 
		 }
		 logger.debug("Reader Thread - " + Thread.currentThread().getName() + "  Done reading file:" + fileName + "  Time taken = " + (System.currentTimeMillis() - startTime));
	}
}
