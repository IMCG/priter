#! bin/bash

HADOOP=/mnt/data/yzhang/hadoop-priter-0.1/bin/hadoop

WORK_DIR=IterativeKaz

#LOCAL_GRAPH=datasets/av_youtube_graph
#NODE=651805

#LOCAL_GRAPH=datasets/av_amazon_graph
#NODE=403394

LOCAL_GRAPH=datasets/kaz_facebook_graph
NODE=319675


#LOCAL_GRAPH=datasets/test
#NODE=4

#NODE=200000

GRAPH=kaz_dataset/trans/iterativeTrans
#RANK=${WORK_DIR}/$LOCAL_RANK
IN=${WORK_DIR}/in
OUT=${WORK_DIR}/out

STARTNODE=8
TOP_K=319675
PARTITIONS=4
ALPHA=$1
STOPTHRESHOLD=1
BETA=0.15
ASYNC=true
PRIEXE=true

: <<'END'
$HADOOP dfs -rmr $WORK_DIR
$HADOOP dfs -rmr $GRAPH
$HADOOP dfs -put $LOCAL_GRAPH $GRAPH
$HADOOP jar iterativeMR.jar preprocess $GRAPH ${WORK_DIR}/subgraphs $PARTITIONS $NODE false
sleep 10
$HADOOP dfs -put temp $IN
END

$HADOOP dfs -rmr $OUT		
$HADOOP jar iterativeMR.jar kaz $IN $OUT ${WORK_DIR}/subgraphs $PARTITIONS $TOP_K $NODE $STARTNODE $ALPHA $STOPTHRESHOLD $ASYNC $PRIEXE $BETA


#for synthetic graph
#$HADOOP jar iterativeMR.jar pagerank $IN $OUT $GRAPH $PARTITIONS $TOP_K $NODE $STARTNODE $ALPHA $STOPTHRESHOLD
