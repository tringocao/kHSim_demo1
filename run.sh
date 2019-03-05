#!/bin/bash
# Put from local to hdfs
hdfs dfs -put /home/minhquang/Documents/hadoop_demo1/input/*  /data/input
hdfs dfs -put /home/minhquang/Documents/hadoop_demo1/src/path.txt  /data/src
# Compile java file
cd /home/minhquang/Documents/hadoop_demo1/src
/home/minhquang/hadoop/bin/hadoop com.sun.tools.javac.Main Shingle.java SpiralClusterBuilder.java
/home/minhquang/hadoop/bin/hadoop com.sun.tools.javac.Main Shingle.java MapReduceJob2.java ClusterItem.java
# Make jar file and run
jar cf SCB.jar SpiralClusterBuilder*.class Shingle.class
jar cf MR2.jar MapReduceJob2*.class Shingle.class ClusterItem.class
/home/minhquang/hadoop/bin/hadoop jar SCB.jar SpiralClusterBuilder /data/src/path.txt /output$1
# Show output
hdfs dfs -cat /output$1/part*
# Copy from HSDF to local
rm /home/minhquang/Documents/hadoop_demo1/res/result
hdfs dfs -get /output$1/part* /home/minhquang/Documents/hadoop_demo1/res/result
# Compile and run 3rd-party process
javac MiddleProcess.java
java MiddleProcess
# Put MidProcessRes to HDFS
hdfs dfs -put /home/minhquang/Documents/hadoop_demo1/res/midProcessRes  /data/input
# Run MR2
/home/minhquang/hadoop/bin/hadoop jar MR2.jar MapReduceJob2 /data/src/pathMR2.txt /output_MR2_$1
# Show output
hdfs dfs -cat /output_MR2_$1/part*