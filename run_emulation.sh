#!/bin/bash

if [ -z "$1" ]
  then
    echo "Trace timestamp cut off not argument supplied (e.g., 20000)"
    exit
fi

TRACE_DIR=trace-`date +%s`
GIT_REV=`git rev-parse HEAD`
GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`

mkdir -p $TRACE_DIR/$GIT_REV

startTimeCutOff=$1

for affinityProportion in 0 50 100;
do
   numNodes=500
   ./gradlew runBenchmark --args="-n $numNodes -f v2-cropped.txt -c 100 -m 200 -t 100 -s ${startTimeCutOff} -p $affinityProportion" &> /tmp/out
   
   expId=`date +%s`
   mkdir -p $TRACE_DIR/$GIT_REV/$expId
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

   echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,affinityProportion" > $TRACE_DIR/$GIT_REV/$expId/metadata
   echo "v2-cropped.txt,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$numNodes,$startTimeCutOff,0,100,$affinityProportion" >> $TRACE_DIR/$GIT_REV/$expId/metadata
done

# Process the above trace (creates a plots/ folder)
python3 process_trace.py $TRACE_DIR data.db
Rscript plot.r

TRACE_DIR=trace-`date +%s`
mkdir -p $TRACE_DIR/$GIT_REV

for numNodes in 500 5000 10000;
do
   affinityProportion=100
   ./gradlew runBenchmark --args="-n ${numNodes} -f v2-cropped.txt -c 100 -m 200 -t 100 -s ${startTimeCutOff} -p ${affinityProportion}" &> /tmp/out
   
   expId=`date +%s`
   mkdir -p $TRACE_DIR/$GIT_REV/$expId
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

   echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,affinityProportion" > $TRACE_DIR/$GIT_REV/$expId/metadata
   echo "v2-cropped.txt,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$numNodes,$startTimeCutOff,0,100,$affinityProportion" >> $TRACE_DIR/$GIT_REV/$expId/metadata
done

# Process the above trace (creates a plots/ folder)
python3 process_trace.py $TRACE_DIR data.db
Rscript plot.r
