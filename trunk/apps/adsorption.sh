#! bin/bash
# Run this shell script to perform adsorption computation on PrIter

HADOOP=../bin/hadoop
JAR=../hadoop-priter-0.1-examples.jar

INPUT=adsorption_dataset		
OUTPUT=adsorption_result
PARTITIONS=4
TOP_K=1000
QPORTION=1
INTERVAL=10000
STOPTHRESHOLD=1	

# for synthetic data input, you should use gendata.sh to generate some synthetic dataset. Note that you should make # of nodes|# of partitions|data dir consistent.
#INPUT=synthetic_data	

# for real data, you should download them and upload them to HDFS first
# amazon co-purchase graph (http://rio.ecs.umass.edu/~yzhang/data/ad_amazon_weightgraph)
#LOCAL_DATA=ad_amazon_weightgraph

# youtube user-view graph (http://rio.ecs.umass.edu/~yzhang/data/ad_youtube_weightgraph)
LOCAL_DATA=ad_youtube_weightgraph

# upload the real dataset to HDFS, it is unnecessary for the synthetic dataset
$HADOOP dfs -rmr $INPUT
$HADOOP dfs -put $LOCAL_DATA $INPUT


$HADOOP dfs -rmr $OUTPUT		# remove the existed output

# perform adsorption computation	
$HADOOP jar $JAR adsorption -p $PARTITIONS -k $TOP_K -q $QPORTION -i $INTERVAL -t $STOPTHRESHOLD $INPUT $OUTPUT 


