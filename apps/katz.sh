#! bin/bash
# Run this shell script to perform Katz metric computation on PrIter

HADOOP=../bin/hadoop
JAR=../hadoop-priter-0.1-examples.jar

INPUT=katz_dataset		
OUTPUT=katz_result
PARTITIONS=4
TOP_K=1000
QPORTION=1
BETA=0.15
INTERVAL=10000
STOPTHRESHOLD=1	

# for synthetic data input, you should use gendata.sh to generate some synthetic dataset. Note that you should make # of nodes|# of partitions|data dir consistent.
#INPUT=synthetic_data	

# for real data, you should download them and upload them to HDFS first
# facebook user  graph (http://rio.ecs.umass.edu/~yzhang/data/katz_facebook_graph)
LOCAL_DATA=katz_facebook_graph


# upload the real dataset to HDFS, it is unnecessary for the synthetic dataset
$HADOOP dfs -rmr $INPUT
$HADOOP dfs -put $LOCAL_DATA $INPUT


$HADOOP dfs -rmr $OUTPUT		# remove the existed output

# perform katz computation	
$HADOOP jar $JAR katz -p $PARTITIONS -k $TOP_K -q $QPORTION -beta $BETA -i $INTERVAL -t $STOPTHRESHOLD $INPUT $OUTPUT 
