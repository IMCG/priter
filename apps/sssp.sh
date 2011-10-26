#! bin/bash
# Run this shell script to perform single source shortest path computation on PrIter

HADOOP=../bin/hadoop
JAR=../hadoop-priter-0.1-examples.jar

INPUT=sssp_dataset		
OUTPUT=sssp_result
PARTITIONS=4
TOP_K=1000
QPORTION=1000
STARTNODE=0
INTERVAL=5000
STOPTHRESHOLD=0	

# for synthetic data, you should use gendata.sh to generate some synthetic dataset. Note that you should make # of nodes|# of partitions|data dir consistent.
#INPUT=synthetic_data	

# for real data, you should download them and upload them to HDFS first
# DBLP author cooperation graph (http://rio.ecs.umass.edu/~yzhang/data/sp_dblp_graph)
LOCAL_DATA=sp_dblp_graph

# Facebook user interaction graph (http://rio.ecs.umass.edu/~yzhang/data/sp_facebook_graph)
#LOCAL_DATA=sp_facebook_graph

# upload the real dataset to HDFS, it is unnecessary for the synthetic dataset
$HADOOP dfs -rmr $INPUT
$HADOOP dfs -put $LOCAL_DATA $INPUT

$HADOOP dfs -rmr $OUTPUT		# remove the existed output

# perform SSSP iteration
$HADOOP jar $JAR sssp -p $PARTITIONS -k $TOP_K -qlen $QPORTION -s $STARTNODE -i $INTERVAL -t $STOPTHRESHOLD $INPUT $OUTPUT 

