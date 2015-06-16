package com.insightdataeng;

import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class WordCountThread extends Thread{
	private static Logger logger = Logger.getLogger(WordCountThread.class);
	private static  WordCount wc = null;
	private String name = null;
	private CountDownLatch latch; 
	private long waitTimeInMillis = 100;
	
	public WordCountThread(String name, WordCount wc, CountDownLatch latch, long waitTimeInMillis) {
		this.name = name;
		this.wc = wc;
		this.latch = latch;
		this.waitTimeInMillis = waitTimeInMillis;
	}
	
	public void run() {
		if (wc == null) {
			logger.error("WordCount instance is null!!");
			countDown();
			return;
		}
		String line = null;
		do {
			try {
				line = wc.getLines().poll(waitTimeInMillis, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} 
			if (line != null) {
				// process the line
				StringTokenizer tokens = new StringTokenizer(line, " ");
				String word = null;
				while (tokens.hasMoreElements()) {
					word = tokens.nextToken();
					word = removeSpecialChars(word);
					wc.incrementCount(word);
				}
				tokens = null;
			}
		} while (!wc.isDoneReadingFiles() || line != null);
		
		countDown();
		logger.debug(name + " done processing lines..");
	}
	
	private String removeSpecialChars(String word) {
		if (word == null) return word;
		return word.toLowerCase().replaceAll("[^a-zA-Z]", "");
	}
	
	public void countDown() {
		if (latch != null) {
			latch.countDown();
		}
	}
}
