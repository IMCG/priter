#! bin/bash

HADOOP=/mnt/data/yzhang/hadoop-priter-0.1/bin/hadoop

WORK_DIR=IterativeConnComp

#for Amazon
LOCAL_GRAPH=cc_amazon_graph
NODE=403394

#for LiveJournal
#LOCAL_GRAPH=cc_livejnl_graph
#NODE=1347571

#for Wiki
#LOCAL_GRAPH=cc_wiki_graph
#NODE=1394385

#for Synthetic
#LOCAL_GRAPH=sp_synthetic_graph
#LOCAL_RANK=sp_synthetic_rank
#NODE=10000
#STARTNODE=0

GRAPH=cc_dataset/iterativestatic
RANK=cc_dataset/iterativestate

IN=${WORK_DIR}/in
OUT=${WORK_DIR}/out
#NODE=3000000
#STARTNODE=0
TOP_K=403394
PARTITIONS=4
EXETOP=100
STOPTIME=240000

: <<'END'
$HADOOP dfs -rmr $WORK_DIR
$HADOOP dfs -rmr cc_dataset
$HADOOP dfs -put datasets/$LOCAL_GRAPH $GRAPH
$HADOOP jar iterativeMR.jar preprocess $GRAPH ${WORK_DIR}/subgraphs $PARTITIONS $NODE false
sleep 10
$HADOOP dfs -put temp $IN
END

$HADOOP dfs -rmr $OUT		
$HADOOP jar iterativeMR.jar conncomp $IN $OUT ${WORK_DIR}/subgraphs $PARTITIONS $TOP_K $NODE $EXETOP $STOPTIME


