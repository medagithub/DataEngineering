package com.insightdataeng;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;



public class WordCount {
	private static Logger logger = Logger.getLogger(WordCount.class);
	private static String cfgFileName = "wordcountconfig.properties"; 
	private static Properties props = null;
	private static String inputDataDir = null;
	private static String outputDataDir = null;
	private static String wcResultsFileName = null;
	private static int numOfWorkerThreads = 10;
	private static int numOfReaderThreads = 50;
	private static int waitTimeInMillis = 100;
	private static LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<String>();	
	private static SortedMap<String, Integer> wcTbl = Collections.synchronizedSortedMap(new TreeMap<String, Integer>());
	private static CountDownLatch latch = null;
	private static boolean doneReadingFiles = false;
	private static byte[] lineSeparatorBytes = System.getProperty("line.separator").getBytes();
	
	
	public void doWordCount() {
		long startTime = System.currentTimeMillis();
		// start worker threads
		startWorkerThreads();
		// start reading text files in the input directory
		readTextFiles();
		
		doneReadingFiles = true;
		// wait for worker threads to finish processing the lines
		try {
			latch.await();
		} catch (Exception e) {
			logger.error(e.getMessage() , e);
		}
		// write the word counts to output directory
		writeResultsToOutputFile();
		logger.debug("Total time taken = " + (System.currentTimeMillis() - startTime));
	}
	
	
	
	public static boolean isDoneReadingFiles() {
		return doneReadingFiles;
	}
	
	public static void incrementCount(String word) {
		if (wcTbl.get(word) != null) {
			wcTbl.put(word, wcTbl.get(word) + 1);
		} else {
			wcTbl.put(word, 1);
		}
	}
	
	public static LinkedBlockingQueue<String> getLines() {
		return lines;
	}
	
	private static boolean  loadProperties(String pFileName) {
		
		if (pFileName != null && !pFileName.isEmpty()) {
			cfgFileName = pFileName;
		}
		logger.debug("Properties file name = " + cfgFileName);
		props = null;
		try {
			File propsFile = new File(cfgFileName);
			if (!propsFile.exists()) {
				logger.error("Error: Properties file not found!! - " + cfgFileName);
				System.exit(-1);
			}
			props = new Properties();
			props.load(new FileInputStream(propsFile));
			
			logger.debug("Successfully loaded properties file:" + propsFile.getAbsolutePath());
			inputDataDir = props.getProperty("wc.input.data.dir", "wc_input");
			outputDataDir = props.getProperty("wc.output.data.dir", "wc_output");
			wcResultsFileName = props.getProperty("wc.result.file.name", "wc_result.txt");
			try {
				numOfWorkerThreads = Integer.parseInt(props.getProperty("num.of.worker.threads"));
			} catch (Exception e) { 
				numOfWorkerThreads = 10;
				logger.error("Invalid value for config property 'num.of.worker.threads' - " + (props.getProperty("num.of.worker.threads")) + "  Using default value:" + numOfWorkerThreads);
			}
			
			try {
				waitTimeInMillis = Integer.parseInt(props.getProperty("wait.time.in.millis"));
			} catch (Exception e) { 
				waitTimeInMillis = 100;
				logger.error("Invalid value for config property 'wait.time.in.millis' - " + (props.getProperty("wait.time.in.millis")) + "  Using default value:" + waitTimeInMillis);
			}
			
			try {
				numOfReaderThreads = Integer.parseInt(props.getProperty("num.of.reader.threads"));
			} catch (Exception e) { 
				numOfReaderThreads = 50;
				logger.error("Invalid value for config property 'num.of.reader.threads' - " + (props.getProperty("num.of.reader.threads")) + "  Using default value:" + numOfReaderThreads);
			}
			
			return true;
		} catch (Exception e) {
			logger.error("Error reading properties file!!! - " + cfgFileName);
			logger.error(e.getMessage(), e);
			return false;
		}
	}
	
	//main method
	//read the config properties e.g. input directory, output directory etc.,
	public static void main(String[] args) {
		if (System.getProperty("config.filename") != null) {
			cfgFileName = System.getProperty("config.filename");
		}
		
		//load config properties
		boolean status = loadProperties(cfgFileName);
		if (!status) {
			logger.error("Error reading properties file!!! - " + cfgFileName);
			System.exit(-1);
		}
		
		new WordCount().doWordCount();	
	}

	private void writeResultsToOutputFile() {
		FileOutputStream ios = null;
		
		File outputDirFile = new File(outputDataDir);
		if (outputDirFile != null && outputDirFile.exists() && !outputDirFile.isDirectory() ) {
			logger.error("Invalid output directory!! Not a folder!! - " + outputDataDir);
			Iterator<Map.Entry<String, Integer>> iter = wcTbl.entrySet().iterator();
			Map.Entry<String, Integer> entry = null;
			while (iter.hasNext()) {
				entry = iter.next();
				System.out.println(entry.getKey() + "\t" + entry.getValue());
			}
			return;
		}
		String outputFileName = outputDataDir + File.separator + wcResultsFileName;
		File file = new File(outputDataDir);		
		try {
			if (file != null && !file.exists()) {
				file.mkdirs();
			}
			outputFileName = file.getAbsolutePath() + File.separator + wcResultsFileName;
			ios = new FileOutputStream(outputFileName );
			Iterator<Map.Entry<String, Integer>> iter = wcTbl.entrySet().iterator();
			Map.Entry<String, Integer> entry = null;
			while (iter.hasNext()) {
				entry = iter.next();
				ios.write((entry.getKey() + "\t" + entry.getValue()).getBytes());
				ios.write(lineSeparatorBytes);
			}
			logger.debug("Successfully wrote the results to " + outputFileName);
		} catch (Exception e) {
			logger.error("Exception while writing word count results to output file:" + outputFileName);
			logger.error(e.getMessage(), e);
		} finally {
			try {
				if (ios != null) ios.close();
			} catch (Exception e) { }
		}
		
	}

	private void readTextFiles() {
		
		File folder = new File(inputDataDir);
		if (folder == null || !folder.isDirectory() || !folder.exists()) {
			logger.error("Invalid input directory - " + inputDataDir + " Either input directory does not exist or it is not a folder!");
			doneReadingFiles = true;
			return;
		}
		File[] files = folder.listFiles();
		ExecutorService service = Executors.newFixedThreadPool(numOfReaderThreads);
		for (int i = 0; i < files.length; i++) {
			service.submit(new FileReaderThread(this, files[i].getAbsolutePath()));
		}
		service.shutdown();
	}

	private  void startWorkerThreads() {
		
		latch = new CountDownLatch(numOfWorkerThreads);
		for (int i = 0; i < numOfWorkerThreads; i++) {
			new WordCountThread("WorkerThread-"+i, this, latch, waitTimeInMillis).start();
		}
	}
}
