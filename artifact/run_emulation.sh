#!/bin/bash

if [ -z "$1" ]
  then
    echo "Trace timestamp cut off not argument supplied (e.g., 20000)"
    exit
fi

SCRIPT_DIR=$(dirname $(readlink -f $0))
GIT_REV=`git rev-parse HEAD`
GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`

startTimeCutOff=$1
traceFile="v2-cropped.txt"

TRACE_DIR=trace-`date +%s`
mkdir -p $TRACE_DIR/$GIT_REV

for affinityProportion in 0 50 100;
do
   # numNodes=500
   numNodes=100

   cd $SCRIPT_DIR/..
   ./gradlew runBenchmark --args="-n ${numNodes} -f ${traceFile} -c 100 -m 200 -t 100 -s ${startTimeCutOff} -p ${affinityProportion}" &> /tmp/out
   cd $SCRIPT_DIR

   expId=`date +%s`
   mkdir -p $TRACE_DIR/$GIT_REV/$expId
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

      echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,affinityProportion" > $TRACE_DIR/$GIT_REV/$expId/metadata
      echo "$traceFile,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$numNodes,$startTimeCutOff,0,100,$affinityProportion" >> $TRACE_DIR/$GIT_REV/$expId/metadata
done

# Process the above trace (creates a plots/ folder)
python3 process_trace.py $TRACE_DIR data.db
Rscript plot.r

# TRACE_DIR=trace-`date +%s`
# mkdir -p $TRACE_DIR/$GIT_REV

# ## for numNodes in 500 5000 10000;
# for numNodes in 50 100 500;
# do
#     affinityProportion=100

#     cd $SCRIPT_DIR/..
#     ./gradlew runBenchmark --args="-n ${numNodes} -f ${traceFile} -c 100 -m 200 -t 100 -s ${startTimeCutOff} -p ${affinityProportion}" &> /tmp/out
#     cd $SCRIPT_DIR
   
#    expId=`date +%s`
#    mkdir -p $TRACE_DIR/$GIT_REV/$expId
#    cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
#    cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

#    echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,affinityProportion" > $TRACE_DIR/$GIT_REV/$expId/metadata
#    echo "$traceFile,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$numNodes,$startTimeCutOff,0,100,$affinityProportion" >> $TRACE_DIR/$GIT_REV/$expId/metadata
# done

# Process the above trace (creates a plots/ folder)
# python3 process_trace.py $TRACE_DIR data.db
# Rscript plot.r
