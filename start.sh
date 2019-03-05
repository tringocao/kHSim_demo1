#!/bin/bash
hdfs namenode -format
start-all.sh
cd /home/minhquang/hadoop/sbin
mr-jobhistory-daemon.sh --config /home/minhquang/hadoop/etc/hadoop/ start historyserver
hdfs dfs -mkdir /data
hdfs dfs -mkdir /data/input
hdfs dfs -mkdir /data/src
