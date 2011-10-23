#! bin/bash

HADOOP=../bin/hadoop

WORK_DIR=PriPageRank

#LOCAL_GRAPH=datasets/pg_synthetic_graph
#NODE=200000
LOCAL_GRAPH=datasets/pg_google_graph
NODE=916428
#AVGDEG=6.6
#LOCAL_GRAPH=datasets/pg_berkstan_graph
#NODE=685231
#EDGE=7626576
#AVGDEG=11.2
#LOCAL_GRAPH=datasets/pg_notredame_graph
#NODE=324721

#NODE=1000000

GRAPH=pg_dataset/trans/iterativeTrans
#RANK=${WORK_DIR}/$LOCAL_RANK
IN=${WORK_DIR}/in
OUT=${WORK_DIR}/out

STARTNODE=100
TOP_K=1000
SORT=false
PARTITIONS=4
ALPHA=$1
STOPTHRESHOLD=1
ASYNC=true
PRIEXE=true
INTERVAL=20000

: <<'END'
$HADOOP dfs -rmr $WORK_DIR
$HADOOP dfs -rmr $GRAPH
$HADOOP dfs -put $LOCAL_GRAPH $GRAPH
$HADOOP jar iterativeMR.jar preprocess $GRAPH ${WORK_DIR}/subgraphs $PARTITIONS $NODE false
sleep 10
$HADOOP dfs -put temp $IN
END

$HADOOP dfs -rmr $OUT		
$HADOOP jar iterativeMR.jar pagerank $IN $OUT ${WORK_DIR}/subgraphs $PARTITIONS $TOP_K $NODE $STARTNODE $ALPHA $STOPTHRESHOLD $ASYNC $PRIEXE $INTERVAL


#for synthetic graph
#$HADOOP jar iterativeMR.jar pagerank $IN $OUT $GRAPH $PARTITIONS $TOP_K $NODE $STARTNODE $ALPHA $STOPTHRESHOLD $ASYNC $PRIEXE $INTERVAL
