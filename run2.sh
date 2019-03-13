#!/bin/bash
# Put from local to hdfs
#hdfs dfs -put /home/minhquang/Documents/hadoop_demo1_2/input/*  /data/input
#hdfs dfs -put /home/minhquang/Documents/hadoop_demo1_2/src/path.txt  /data/src
cd /home/minhquang/Documents/hadoop_demo1_2/src
# Run jar file
/home/minhquang/hadoop/bin/hadoop jar SCB.jar SpiralClusterBuilder /data/src/path.txt /output$1
# Show output
hdfs dfs -cat /output$1/part*
# Copy from HSDF to local
rm /home/minhquang/Documents/hadoop_demo1_2/res/result
hdfs dfs -get /output$1/part* /home/minhquang/Documents/hadoop_demo1_2/res/result
# Run 3rd-party process
java MiddleProcess
# Put MidProcessRes to HDFS
hdfs dfs -put /home/minhquang/Documents/hadoop_demo1_2/res/midProcessRes  /data/input
