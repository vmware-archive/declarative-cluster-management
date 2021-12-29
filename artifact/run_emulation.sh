#!/bin/bash
set -x

SCRIPT_DIR=$(dirname "$(readlink -f $0)")
GIT_REV=$(git rev-parse HEAD)
ROOT_EXP_ID=$(date +%s)

TRACE_DIR="traces-$ROOT_EXP_ID"
DB_DIR="db-$ROOT_EXP_ID"
PLOT_DIR="plots-$ROOT_EXP_ID"
mkdir -p $TRACE_DIR
mkdir -p $DB_DIR
mkdir -p $PLOT_DIR

# optional argument to declare full-parameters run
runMode=${1:-short}
if [ "$runMode" = "full" ]; then
    speedup=100
    startCutOff=5000
    nodesSparse=(100 1000 10000)
    nodesDense=(30 100 300 1000 3000 10000 30000)
else
    speedup=100
    startCutOff=1000
    nodesSparse=(20 100 500)
    nodesDense=(20 40 80 160 320 640)
fi
# traceFile="v2-cropped.txt"
traceFile="generated-data.txt"
schedulers=("dcm-scheduler" "dcm-scheduler-scoped")
scenarios=("scenario 1" "scenario 2" "scenario 3")
scenarioPodInterAf=(0 50 100)


# 1A Run for Scalability: cluster size vs latency
for scheduler in "${schedulers[@]}"; do
  for nodes in "${nodesDense[@]}"; do
    echo "1A: ${scheduler} scheduler, ${nodes} nodes, ${scenarioPodInterAf[0]} podInterAf"

    cd $SCRIPT_DIR/..
    if [ $scheduler == "dcm-scheduler-scoped" ]; then
      cmd="./gradlew runBenchmark --args='-n ${nodes} -f ${traceFile} -c 100 -m 200 -t ${speedup} -s ${startCutOff} -p ${scenarioPodInterAf[0]} -S' &> /tmp/out"
      eval $cmd
    else
      cmd="./gradlew runBenchmark --args='-n ${nodes} -f ${traceFile} -c 100 -m 200 -t ${speedup} -s ${startCutOff} -p ${scenarioPodInterAf[0]}' &> /tmp/out"
      eval $cmd
    fi
    cd $SCRIPT_DIR

    expTraceDir="$TRACE_DIR/1A-scalability-clustersize/$(date +%s)"
    mkdir -p $expTraceDir
    cp /tmp/out $expTraceDir/trace

    echo "$cmd" >> "$expTraceDir/cmd"
    echo "workload,schedulerName,solver,kubeconfig,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,scenario,affinityProportion" > $expTraceDir/metadata
    echo "$traceFile,$scheduler,ORTOOLS,local,$GIT_REV,$nodes,$startCutOff,0,100,${scenarios[0]},${scenarioPodInterAf[0]}" >> $expTraceDir/metadata
  done
done

# Process traces and store into a db
python3 process_trace.py "$TRACE_DIR/1A-scalability-clustersize" "$DB_DIR/data1A.db"
# Produce plots and latex tables
mkdir -p "$PLOT_DIR/1A-scalability-clustersize"
Rscript plot.r $runMode "$DB_DIR/data1A.db" "$PLOT_DIR/1A-scalability-clustersize"


# 1B Run for Scalability: different scenarios latency ECDF
for scheduler in "${schedulers[@]}"; do
  for nodes in "${nodesSparse[@]}"; do
    for scenarioIdx in "${!scenarios[@]}"; do
      echo "1B: ${scheduler} scheduler, ${nodes} nodes, ${scenarios[scenarioIdx]}"

      cd $SCRIPT_DIR/..
      if [ $scheduler == "dcm-scheduler-scoped" ]; then
        cmd="./gradlew runBenchmark --args='-n ${nodes} -f ${traceFile} -c 100 -m 200 -t ${speedup} -s ${startCutOff} -p ${scenarioPodInterAf[scenarioIdx]} -S' &> /tmp/out"
        eval $cmd
      else
        cmd="./gradlew runBenchmark --args='-n ${nodes} -f ${traceFile} -c 100 -m 200 -t ${speedup} -s ${startCutOff} -p ${scenarioPodInterAf[scenarioIdx]}' &> /tmp/out"
        eval $cmd
      fi
      cd $SCRIPT_DIR

      expTraceDir="$TRACE_DIR/1B-scalability-ecdf/$(date +%s)"
      mkdir -p $expTraceDir
      cp /tmp/out $expTraceDir/trace

      echo "$cmd" >> "$expTraceDir/cmd"
      echo "workload,schedulerName,solver,kubeconfig,dcmGitCommitId,numNodes,startTimeCutOff,percentageOfNodesToScoreValue,timeScaleDown,scenario,affinityProportion" > $expTraceDir/metadata
      echo "$traceFile,$scheduler,ORTOOLS,local,$GIT_REV,$nodes,$startCutOff,0,100,${scenarios[scenarioIdx]},${scenarioPodInterAf[scenarioIdx]}" >> $expTraceDir/metadata
    done
  done
done

# Process traces and store into a db
python3 process_trace.py "$TRACE_DIR/1B-scalability-ecdf" "$DB_DIR/data1B.db"
# Produce plots and latex tables
mkdir -p "$PLOT_DIR/1B-scalability-ecdf"
Rscript plot.r $runMode "$DB_DIR/data1B.db" "$PLOT_DIR/1B-scalability-ecdf"

