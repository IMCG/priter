#! bin/bash

HADOOP=/mnt/data/yzhang/hadoop-priter-0.1/bin/hadoop

WORK_DIR=IterativeShortestPath

#for DBLP
#LOCAL_GRAPH=sp_dblp_graph
#NODE=310555
#STARTNODE=23845

#for Facebook
#LOCAL_GRAPH=sp_facebook_graph2
#NODE=1204003
#STARTNODE=242849

#for Livejounal
#LOCAL_GRAPH=sp_livejnl_graph
#NODE=4847570
#STARTNODE=0

#for roadCA
#LOCAL_GRAPH=sp_roadCA_graph
#NODE=1965206
#STARTNODE=0
#NODE=200000

#for Synthetic
#LOCAL_GRAPH=sp_synthetic_graph
#LOCAL_RANK=sp_synthetic_rank
#NODE=10000
#STARTNODE=0

GRAPH=sp_dataset/iterativestatic
RANK=sp_dataset/iterativestate

NODE=500000
STARTNODE=0

IN=${WORK_DIR}/in
OUT=${WORK_DIR}/out
TOP_K=500000
PARTITIONS=4
QUEUELEN=$1
STOPTHRESHOLD=0
ASYNC=true
PRIEXE=true
INTERVAL=5000

: <<'END'
$HADOOP dfs -rmr $WORK_DIR
$HADOOP dfs -rmr sp_dataset
$HADOOP dfs -put datasets/$LOCAL_GRAPH $GRAPH
$HADOOP jar iterativeMR.jar preprocess $GRAPH ${WORK_DIR}/subgraphs $PARTITIONS $NODE false
sleep 10
$HADOOP dfs -put temp $IN
END

$HADOOP dfs -rmr $OUT		
#$HADOOP jar iterativeMR.jar bsearch $IN $OUT ${WORK_DIR}/subgraphs $PARTITIONS $TOP_K $NODE $STARTNODE $QUEUELEN $STOPTHRESHOLD $ASYNC $PRIEXE $INTERVAL
$HADOOP jar iterativeMR.jar bsearch $IN $OUT $GRAPH $PARTITIONS $TOP_K $NODE $STARTNODE $QUEUELEN $STOPTHRESHOLD $ASYNC $PRIEXE $INTERVAL

