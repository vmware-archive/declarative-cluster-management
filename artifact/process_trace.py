#!/bin/python

import sys
import glob
import sqlite3
import time
import datetime
import csv

if (len(sys.argv) != 3):
   print ("Usage: process.py <trace folder> <database to store results in>")
   sys.exit(1)

folderToOpen = sys.argv[1]
database = sys.argv[2]
conn = sqlite3.connect(database)
traceFolders = glob.glob(folderToOpen + "/*/*/")

conn.execute("drop table if exists params")
conn.execute("drop table if exists pods_over_time")
conn.execute("drop table if exists scheduler_trace")
conn.execute("drop table if exists dcm_table_access_latency")
conn.execute("drop table if exists dcm_metrics")
conn.execute("drop table if exists event_trace")

conn.execute('''
    create table params
    (
        expId integer not null primary key,
        workload varchar(100) not null,
        scheduler varchar(100) not null,
        solver varchar(100) not null,
        kubeconfig integer not null,
        dcmGitBranch integer not null,
        dcmGitCommitId varchar(100) not null,
        numNodes integer not null,
        startTimeCutOff integer not null,
        percentageOfNodesToScoreValue integer not null,
        timeScaleDown integer not null,
        affinityProportion integer not null
    )
''')

conn.execute('''
    create table pods_over_time
    (
        expId integer not null,
        podName varchar(100) not null,
        status varchar(100) not null,
        event varchar(100) not null,
        eventTime integer not null,
        nodeName varchar(100) not null,
        foreign key(expId) references params(expId)
    )
''')

conn.execute('''
    create table scheduler_trace
    (
        expId integer not null,
        podName varchar(100) not null,
        bindTime numeric not null,
        batchId integer not null,
        scheduler varchar(100) not null,
        foreign key(expId) references params(expId)
    )
''')


conn.execute('''
    create table dcm_table_access_latency
    (
        expId integer not null,
        batchId integer not null,
        tableName varchar(50) not null,
        fetchLatency integer not null,
        reflectLatency integer not null,
        foreign key(expId) references params(expId)
    )
''')

conn.execute('''
    create table dcm_metrics
    (
        expId integer not null,
        batchId integer not null,
        dcmSolveTime integer not null,
        fetchcount integer not null,
        modelCreationLatency integer not null,
        variablesBeforePresolve integer not null,
        variablesBeforePresolveObjective integer not null,
        variablesAfterPresolve integer not null,
        variablesAfterPresolveObjective integer not null,
        presolveTime numeric not null,
        propagations integer not null,
        integerPropagations integer not null,
        orToolsUserTime integer not null,
        orToolsWallTime integer not null,
        databaseLatencyTotal integer not null,
        foreign key(expId) references params(expId)
    )
''')

def convertRunningSinceToSeconds(runningSince):
   minutes = 0
   if ("m" in runningSince):
      minutes = int(runningSince.split("m")[0])
      seconds = 0
      if ("s" in runningSince):
         seconds = int(runningSince.split("m")[1][:-1])
      return (minutes * 60) + seconds
   assert "s" in runningSince
   return int(runningSince[:-1])

def convertK8sTimestamp(k8stimestamp):
   split = k8stimestamp.split("T")
   date = split[0]
   timeSplit = split[1].split(".")
   hhmmss = timeSplit[0]
   # nanosecond representation skips trailing zeros
   nanos = 0
   if (len(timeSplit) >= 2):
      nanos = timeSplit[1][:-1].ljust(9, '0')
   else:
      # events trace does not have nanos
      assert hhmmss[-1] == "Z"
      hhmmss = hhmmss[:-1]
   date_str = "%s %s" % (date, hhmmss)
   timestamp = time.mktime(datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timetuple())
   return (int(timestamp) * 1e9) + int(nanos)


for trace in traceFolders:
    # First, we populate the params table
    expId = trace.split('/')[2]
    expParams = {}
    expParams["expId"] = expId

    paramFiles = glob.glob(trace + "metadata")
    assert len(paramFiles) == 1

    print("experiment ID: ", end='')
    print(expId)

    # Add params file in addition
    with open(paramFiles[0]) as paramFile:
        reader = csv.reader(paramFile, delimiter=',')
        header = next(reader)
        data = next(reader)
        for i in range(len(header)):
            expParams[header[i]] = data[i]

    conn.execute("insert into params values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 (expParams["expId"],
                  expParams["workload"],
                  expParams["schedulerName"],
                  expParams["solver"],
                  expParams["kubeconfig"],
                  expParams["dcmGitBranch"],
                  expParams["dcmGitCommitId"],
                  expParams["numNodes"],
                  expParams["startTimeCutOff"],
                  expParams["percentageOfNodesToScoreValue"],
                  expParams["timeScaleDown"],
                  expParams["affinityProportion"]))


    # Get the list of pods
    pods = {}
    with open(trace + "/workload_output") as workloadOutput:
        for line in workloadOutput:
            if ("org.dcm.WorkloadGeneratorIT" in line and "PodName" in line):
                splitLine = line.split("org.dcm.WorkloadGeneratorIT")[-1].split(" ")
                eventTime = splitLine[3][:-1]
                podName = splitLine[7][:-1]
                nodeName = splitLine[9][:-1]
                status = splitLine[11][:-1]
                event = splitLine[13].strip()
                conn.execute("insert into pods_over_time values (?, ?, ?, ?, ?, ?)",
                             (expParams["expId"], podName, status, event, eventTime, nodeName))

    dcmSchedulerFile = glob.glob(trace + "/dcm_scheduler_trace")
    if (len(dcmSchedulerFile) > 0):
        with open(dcmSchedulerFile[0]) as traceFile:
            batchId = 1
            metrics = {}
            variablesBeforePresolve = False
            for line in traceFile:
                if ("Fetchcount is" in line):
                    metrics["fetchcount"] = line.split()[-1]

                if ("updateDataFields for" in line):
                    split = line.split()
                    tableName = split[8]
                    latencyToDb = split[10]
                    latencyToReflect = split[19]

                    conn.execute("insert into dcm_table_access_latency values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, tableName, latencyToDb, latencyToReflect))

                if ("compiler.updateData()" in line):
                    split = line.split()
                    latency = split[7][:-2] # remove ns
                    metrics["databaseLatencyTotal"] = latency

                if ("Model creation:" in line and not "println" in line):
                    split = line.split()
                    latency = split[5]
                    metrics["modelCreationLatency"] = latency

                if ("#Variables" in line and variablesBeforePresolve == False):
                    split = line.split()
                    numVariablesTotal = split[1]
                    numVariablesObjective = split[2][1:] # Remove opening bracket
                    variablesBeforePresolve = True
                    metrics["variablesBeforePresolve"] = numVariablesTotal
                    metrics["variablesBeforePresolveObjective"] = numVariablesObjective
                    
                elif ("#Variables" in line and variablesBeforePresolve == True):
                    split = line.split()
                    numVariablesTotal = split[1]
                    numVariablesObjective = split[2][1:]
                    variablesBeforePresolve = False
                    metrics["variablesAfterPresolve"] = numVariablesTotal
                    metrics["variablesAfterPresolveObjective"] = numVariablesObjective

                if ("Starting Search" in line):
                    split = line.split()
                    latency = split[3][:-1]
                    metrics["preSolveTime"] = latency

                if ("propagations:" in line and not "integer_propagations" in line):
                    split = line.split()
                    propagations = split[1]
                    metrics["propagations"] = propagations

                if ("integer_propagations:" in line):
                    split = line.split()
                    integerPropagations = split[1]
                    metrics["integerPropagations"] = integerPropagations

                if ("walltime" in line):
                    split = line.split()
                    latency = split[1]
                    metrics["orToolsWallTime"] = latency

                if ("usertime" in line):
                    split = line.split()
                    latency = split[1]
                    metrics["orToolsUserTime"] = latency

                if ("Solver has run successfully" in line):
                    split = line.split()
                    latency = split[10][:-3] # Remove ns and period

                    metrics["dcmSolveTime"] = latency
                    conn.execute("insert into dcm_metrics values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                 (expParams["expId"],
                                  batchId,
                                  metrics["dcmSolveTime"],
                                  metrics["fetchcount"],
                                  metrics["modelCreationLatency"],
                                  metrics["variablesBeforePresolve"],
                                  metrics["variablesBeforePresolveObjective"],
                                  metrics["variablesAfterPresolve"],
                                  metrics["variablesAfterPresolveObjective"],
                                  metrics["preSolveTime"],
                                  metrics["propagations"],
                                  metrics["integerPropagations"],
                                  metrics["orToolsUserTime"],
                                  metrics["orToolsWallTime"],
                                  metrics["databaseLatencyTotal"]
                                 ))

                if ("Scheduling decision for pod" in line):
                    split = line.split("Scheduling decision for pod")[-1].split()
                    pod, batch, bindTime = split[0], split[5], split[-1]

                    conn.execute("insert into scheduler_trace values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], pod, bindTime, batch, "dcm-scheduler"))
                    batchId = int(batch) + 1


    defaultSchedulerFile = glob.glob(trace + "/default_scheduler_trace")
    batchId = 0
    if (len(defaultSchedulerFile) > 0):
        with open(defaultSchedulerFile[0]) as traceFile:
            for line in traceFile:
                if ("About to try and schedule pod" in line):
                    split = line.split()
                    startTime, pod = split[0], split[-1]
                    pod = pod.split("/")[-1]  # pods here are named <namespace>/<podname>
                    if (pod not in pods):
                        pods[pod] = {}
                    pods[pod]["startTime"] = convertK8sTimestamp(startTime)
                    pods[pod]["startLine"] = split

                if ("Attempting to bind" in line):
                    split = line.split()
                    bindTime, pod = split[0], split[-3]
                    assert pod in pods
                    pods[pod]["bindTime"] = convertK8sTimestamp(bindTime)
                    pods[pod]["endLine"] = split
                    assert pods[pod]["bindTime"] > pods[pod]["startTime"], pods[pod]
                    conn.execute("insert into scheduler_trace values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], pod, pods[pod]["bindTime"] - pods[pod]["startTime"],
                                  batchId, "default-scheduler"))
                    batchId += 1
    conn.commit()
