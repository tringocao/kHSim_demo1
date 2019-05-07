#!/bin/bash
path=$PWD
hadoopvar=/home/minhquang/hadoop/bin/hadoop
#$1 = times 

# Remove exist file in hdfs
#hdfs dfs -rm /data/input/*
hdfs dfs -rm /data/input2/*
hdfs dfs -rm /data/src/*

# Put from local to hdfs
hdfs dfs -put $path/input/*  /data/input
hdfs dfs -put $path/src/query.txt  /data/src

# Run
cd $path/src
$hadoopvar jar SCB.jar SpiralClusterBuilder /data/input/ /output0$1

# Copy from HSDF to local
rm $path/res/*
hdfs dfs -get /output0$1/part* $path/res

# Copy output MR-1 to input2
hdfs dfs -cp /output0$1/part* /data/input2

# Run MR-2
$hadoopvar jar MR2.jar MapReduceJob2 /data/input2 /output-MR2-0$1 /data/src/query.txt

# Copy from HSDF to local
hdfs dfs -get /output-MR2-0$1/part* $path/res/FINAL.txt

