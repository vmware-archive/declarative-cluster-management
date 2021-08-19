#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $0))
GIT_REV=`git rev-parse HEAD`
IT_BRANCH=`git rev-parse --abbrev-ref HEAD`

# 1st optional argument
mode=${1:-short}
if [ "$mode" = "full" ]; then
    cutOff=10000
    exp1Nodes=500
    exp2NodesList=(500 5000 10000)
else 
    cutOff=1000
    exp1Nodes=100
    exp2NodesList=(100 500 1000)
fi
# 2nd optional argument
startTimeCutOff=${2:-$cutOff}

traceFile="v2-cropped.txt"
TRACE_DIR=trace-`date +%s`
mkdir -p $TRACE_DIR/$GIT_REV


# plot for varying affinity 
for affinityProportion in 0 50 100;
do
    cd $SCRIPT_DIR/..
    ./gradlew runBenchmark --args="-n ${exp1Nodes} -f ${traceFile} -c 100 -m 200 -t 100 -s ${startTimeCutOff} -p ${affinityProportion}" &> /tmp/out
    cd $SCRIPT_DIR

    expId=`date +%s`
    mkdir -p $TRACE_DIR/$GIT_REV/$expId
    cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
    cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

    echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,affinityProportion" > $TRACE_DIR/$GIT_REV/$expId/metadata
    echo "$traceFile,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$exp1Nodes,$startTimeCutOff,0,100,$affinityProportion" >> $TRACE_DIR/$GIT_REV/$expId/metadata
done

# Process the above trace (creates a plots/ folder)
python3 process_trace.py $TRACE_DIR dataF.db
Rscript plot.r dataF.db $mode


TRACE_DIR=trace-`date +%s`
mkdir -p $TRACE_DIR/$GIT_REV

for exp2Nodes in ${exp2NodesList[@]};
do
    affinityProportion=100

    cd $SCRIPT_DIR/..
    ./gradlew runBenchmark --args="-n ${exp2Nodes} -f ${traceFile} -c 100 -m 200 -t 100 -s ${startTimeCutOff} -p ${affinityProportion}" &> /tmp/out
    cd $SCRIPT_DIR
   
    expId=`date +%s`
    mkdir -p $TRACE_DIR/$GIT_REV/$expId
    cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
    cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

    echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,affinityProportion" > $TRACE_DIR/$GIT_REV/$expId/metadata
    echo "$traceFile,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$exp2Nodes,$startTimeCutOff,0,100,$affinityProportion" >> $TRACE_DIR/$GIT_REV/$expId/metadata
done

# Process the above trace (creates a plots/ folder)
python3 process_trace.py $TRACE_DIR dataN.db
Rscript plot.r dataN.db $mode
