#! bin/bash
# Run this shell script to perform PageRank computation on PrIter

HADOOP=../bin/hadoop
JAR=../hadoop-priter-0.1-examples.jar

INPUT=pagerank_dataset		
OUTPUT=pagerank_result
PARTITIONS=4
TOP_K=1000
QPORTION=0.2
INTERVAL=20000
STOPTHRESHOLD=1	

# for synthetic data input, you should use gendata.sh to generate some synthetic dataset. Note that you should make # of nodes|# of partitions|data dir consistent.
#INPUT=synthetic_data	

# for real data, you should download them and upload them to HDFS first
# google web graph (http://rio.ecs.umass.edu/~yzhang/data/pg_google_graph)
#LOCAL_DATA=pg_google_graph

# berkstan web graph (http://rio.ecs.umass.edu/~yzhang/data/pg_berkstan_graph)
LOCAL_DATA=pg_berkstan_graph

# upload the real dataset to HDFS, it is unnecessary for the synthetic dataset
$HADOOP dfs -rmr $INPUT
$HADOOP dfs -put $LOCAL_DATA $INPUT


$HADOOP dfs -rmr $OUTPUT		# remove the existed output

# perform pagerank computation	
$HADOOP jar $JAR pagerank -p $PARTITIONS -k $TOP_K -q $QPORTION -i $INTERVAL -t $STOPTHRESHOLD $INPUT $OUTPUT 

