#!/bin/bash
path=$PWD
hadoopvar=/home/minhquang/hadoop/bin/hadoop
#$1 = times 

# Compile java file
cd $path/src
$hadoopvar com.sun.tools.javac.Main SpiralClusterBuilder.java kHSimHelper.java ClusterItem.java
jar cf SCB.jar SpiralClusterBuilder*.class kHSimHelper.class ClusterItem.class

$hadoopvar com.sun.tools.javac.Main MapReduceJob2.java kHSimHelper.java ClusterItem.java
jar cf MR2.jar MapReduceJob2*.class kHSimHelper.class ClusterItem.class

