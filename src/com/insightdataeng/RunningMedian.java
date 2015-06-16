package com.insightdataeng;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;


public class RunningMedian {
	private static Logger logger = Logger.getLogger(RunningMedian.class);
	private static String cfgFileName = "runningmedianconfig.properties"; 
	private static Properties props = null;
	private static String inputDataDir = null;
	private static String outputDataDir = null;
	private static String rmResultsFileName = null;
	private static int numOfReaderThreads = 50;
	private static int waitTimeInMillis = 100;	
	private static SortedMap<Integer, List<Integer>> wordCountsPerFile = null;
	private static List<Double> rMedians = null;
	private static byte[] lineSeparatorBytes = System.getProperty("line.separator").getBytes();
    public static Queue<Integer> rightQueue = null;
    public static Queue<Integer> leftQueue = null;
    public static int numOfMedians;
    private static int numberOfFilesProcessed = 0;
    
    
    public RunningMedian() {
    	rightQueue = new PriorityQueue<Integer>();
    	leftQueue = new PriorityQueue<Integer>(100, this.new DescendingComparator());
    	wordCountsPerFile = Collections.synchronizedSortedMap(new TreeMap<Integer, List<Integer>>());
    	rMedians = new Vector<Double>();
    }
    
    
    public void incrementNumberOfFilesProcessed() {
    	numberOfFilesProcessed++;
    }
    
    public void addNumber(Integer num) {
        leftQueue.add(num);
        try {
	        if (numOfMedians%2 == 0) {
	            if (rightQueue.isEmpty()) {
	            	numOfMedians++;
	                return;
	            }
	            else if (leftQueue.peek() > rightQueue.peek()) {
	                Integer maxHeapRoot = leftQueue.poll();
	                Integer minHeapRoot = rightQueue.poll();
	                leftQueue.add(minHeapRoot);
	                rightQueue.add(maxHeapRoot);
	            }
	        } else {
	            rightQueue.add(leftQueue.poll());
	        }
	        numOfMedians++;
        } catch (Exception e) {
        	logger.error("Exception while adding a number to the list..");
        	logger.error(e.getMessage(), e);
        } finally {
        	rMedians.add(getMedian());
        	//logger.debug("Added number:" + num + "  max heap size = " + leftQueue.size() + "  min heap size = " + rightQueue.size() + " numOfElements = " + numOfMedians + " maxHeap head = " + leftQueue.peek() + "  minHeap head = " + rightQueue.peek());
        }
        
    }

    public Double getMedian() {
        if (numOfMedians%2 != 0)
            return new Double(leftQueue.peek());
        else
            return (leftQueue.peek() + rightQueue.peek()) / 2.0;
    }


    private void addNumbers(List<Integer> wcList) {
    	if (wcList == null) return;
    
    	Iterator<Integer> iter = wcList.iterator(); 
		while (iter.hasNext()) {
			addNumber(iter.next());
		}
    }
	public void calculateRunningMedians() {
		long startTime = System.currentTimeMillis();

		// start reading text files in the input directory
		readTextFiles();

		// write the word counts to output directory
		writeResultsToOutputFile();
		logger.debug("Total time taken = " + (System.currentTimeMillis() - startTime));
	}
	
	
	public SortedMap<Integer, List<Integer>> getWordCountsPerFileMap() {
		return  wordCountsPerFile;
	}
	
	private void readTextFiles() {
		
		File folder = new File(inputDataDir);
		if (folder == null || !folder.isDirectory() || !folder.exists()) {
			logger.error("Invalid input directory - " + inputDataDir + " Either input directory does not exist or it is not a folder!");
			return;
		}
		File[] files = folder.listFiles();
		Arrays.sort(files);


		ExecutorService service = Executors.newFixedThreadPool(numOfReaderThreads);
		int count = 0;
		for (int i = 0; i < files.length; i++) {
			if (files[i].isDirectory()) continue;
			service.submit(new FileReaderThread2(this, files[i].getAbsolutePath(), count));
			count++;
		}
		
		service.shutdown();
		int fileIndex = 0;
		while (fileIndex < count) {
			List<Integer> wcList = getWordCountsPerFileMap().get(fileIndex);
			if (wcList != null) {
				logger.debug("Adding word counts for index:" + fileIndex + "  size = " + wcList.size());
				addNumbers(wcList);
				fileIndex++;
			} else {
				try {
					Thread.sleep(100);
				} catch (Exception e) {				
				}
			}
		}
		
		
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
			rmResultsFileName = props.getProperty("rm.result.file.name", "med_result.txt");

			
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
	
    private class DescendingComparator implements Comparator<Integer> {

        public int compare(Integer num1, Integer num2) {
            return num2 - num1;
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
		
		RunningMedian rm = new RunningMedian();
		rm.calculateRunningMedians();
	}

	private void writeResultsToOutputFile() {
		FileOutputStream ios = null;
		
		File outputDirFile = new File(outputDataDir);
		if (outputDirFile != null && outputDirFile.exists() && !outputDirFile.isDirectory() ) {
			logger.error("Invalid output directory!! Not a folder!! - " + outputDataDir);
			Iterator<Double> iter = rMedians.iterator();
			while (iter.hasNext()) {
				System.out.println(iter.next());
			}
			return;
		}
		String outputFileName = outputDataDir + File.separator + rmResultsFileName;
		File file = new File(outputDataDir);		
		try {
			if (file != null && !file.exists()) {
				file.mkdirs();
			}
			outputFileName = file.getAbsolutePath() + File.separator + rmResultsFileName;
			ios = new FileOutputStream(outputFileName);
			Iterator<Double> iter = rMedians.iterator();
			while (iter.hasNext()) {
				ios.write(("" + iter.next()).getBytes());
				ios.write(lineSeparatorBytes);
			}
			logger.debug("Successfully wrote the results to " + outputFileName);
		} catch (Exception e) {
			logger.error("Exception while writing running median results to output file:" + outputFileName);
			logger.error(e.getMessage(), e);
		} finally {
			try {
				if (ios != null) ios.close();
			} catch (Exception e) { }
		}
		
	}

}
