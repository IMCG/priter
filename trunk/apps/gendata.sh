#! bin/bash
# generate synthetic lognormal graph

HADOOP=../bin/hadoop
JAR=../hadoop-priter-0.1-examples.jar

OUT_DIR=synthetic_data
NODE=100000
PARTITIONS=4
TYPE=weighted

$HADOOP dfs -rmr $OUT_DIR
$HADOOP jar $JAR gengraph -p $PARTITIONS -n $NODE -t $TYPE -degree_mu 0.5 -degree_sigma 2 -weight_mu 0.5 -weight_sigma 1 $OUT_DIR
