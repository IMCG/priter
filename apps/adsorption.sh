#! bin/bash

HADOOP=/mnt/data/yzhang/hadoop-priter-0.1/bin/hadoop

WORK_DIR=IterativeAdsorption

#for test
#LOCAL_GRAPH=test
#NODE=4

#for youtube
#LOCAL_GRAPH=ad_youtube_graph
#NODE=113367

#for amazon
#LOCAL_GRAPH=ad_amazon_graph
#NODE=50000

#for youtube
LOCAL_GRAPH=ad_facebook_graph
NODE=80000

#for Synthetic
#LOCAL_GRAPH=sp_synthetic_graph
#LOCAL_RANK=sp_synthetic_rank
#NODE=10000
#STARTNODE=0

GRAPH=ad_dataset/iterativestatic
RANK=ad_dataset/iterativestate


IN=${WORK_DIR}/in
OUT=${WORK_DIR}/out
STARTNODES=100
TOP_K=1000
PARTITIONS=4
ALPHA=0.1
STOPTIME=-1

: <<'END'
$HADOOP dfs -rmr $WORK_DIR
$HADOOP dfs -rmr ad_dataset
$HADOOP dfs -put datasets/$LOCAL_GRAPH $GRAPH
$HADOOP jar iterativeMR.jar preprocess $GRAPH ${WORK_DIR}/subgraphs $PARTITIONS $NODE false
sleep 10
$HADOOP dfs -put temp $IN
END

$HADOOP dfs -rmr $OUT		
$HADOOP jar iterativeMR.jar adsorption $IN $OUT ${WORK_DIR}/subgraphs $PARTITIONS $TOP_K $NODE $STARTNODES $ALPHA $STOPTIME


