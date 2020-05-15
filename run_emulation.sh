#!/bin/bash


TRACE_DIR=trace-`date +%s`
GIT_REV=`git rev-parse HEAD`
GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`

mkdir -p $TRACE_DIR/$GIT_REV

for numNodes in 500 5000 10000;
do
   startTimeCutOff=200000
   java -cp k8s-scheduler/target/k8s-scheduler-1.0-SNAPSHOT-tests.jar:k8s-scheduler/target/k8s-dcm-scheduler.jar org.dcm.EmulatedClusterTest -n $numNodes -f v2-cropped.txt -c 100 -m 200 -t 100 -s $startTimeCutOff &> /tmp/out
   
   expId=`date +%s`
   mkdir -p $TRACE_DIR/$GIT_REV/$expId
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/workload_output
   cp /tmp/out $TRACE_DIR/$GIT_REV/$expId/dcm_scheduler_trace

   echo "workload,schedulerName,solver,kubeconfig,dcmGitBranch,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown" > $TRACE_DIR/$GIT_REV/$expId/metadata
   echo "v2-cropped.txt,dcm-scheduler,ORTOOLS,local,$GIT_BRANCH,$GIT_REV,$numNodes,$startTimeCutOff,0,100" >> $TRACE_DIR/$GIT_REV/$expId/metadata
done
