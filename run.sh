#!/usr/bin/env bash

./jdk1.7.0_79/bin/javac -classpath .:./jdk1.7.0_79/lib/tools.jar:./src:./lib/log4j-1.2.16.jar:./resources ./src/com/insightdataeng/*.java
./jdk1.7.0_79/bin/java -classpath .:./jdk1.7.0_79/lib/tools.jar:./src:./lib/log4j-1.2.16.jar:./resources -Dconfig.filename=./resources/wordcountconfig.properties com.insightdataeng.WordCount 
./jdk1.7.0_79/bin/java -classpath .:./jdk1.7.0_79/lib/tools.jar:./src:./lib/log4j-1.2.16.jar:./resources -Dconfig.filename=./resources/runningmedianconfig.properties com.insightdataeng.RunningMedian 
