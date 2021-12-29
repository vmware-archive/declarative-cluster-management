#!/usr/bin/python

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
traceFolders = glob.glob(folderToOpen + "/*/")
database = sys.argv[2]
conn = sqlite3.connect(database)

conn.execute("drop table if exists params")
conn.execute("drop table if exists pod_events")
conn.execute("drop table if exists scheduler_trace")
conn.execute("drop table if exists dcm_table_access_latency")
conn.execute("drop table if exists dcm_metrics")
conn.execute("drop table if exists scope_fraction")
conn.execute("drop table if exists problem_size")

conn.execute('''
    create table params
    (
        expId integer not null primary key,
        workload varchar(100) not null,
        scheduler varchar(100) not null,
        solver varchar(100) not null,
        kubeconfig integer not null,
        dcmGitCommitId varchar(100) not null,
        numNodes integer not null,
        startTimeCutOff integer not null,
        percentageOfNodesToScoreValue integer not null,
        timeScaleDown integer not null,
        scenario integer not null,
        affinityProportion integer not null
    )
''')

conn.execute('''
    create table pod_events
    (
        expId integer not null,
        batchId integer not null,
        podName varchar(100) not null,
        uid varchar(100) not null,
        event varchar(100) not null,
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

# FIXME: missing scope latency
conn.execute('''
    create table dcm_metrics
    (
        expId integer not null,
        batchId integer not null,
        dcmSolveTime integer not null,
        fetchcount integer not null,
        scopeLatency integer not null,
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

# FIXME: missing scope fraction
conn.execute('''
    create table scope_fraction
    (
        expId integer not null,
        batchId integer not null,
        scopeFraction double not null,
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

    print("Tarce processor running for experiment ID: ", end='')
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
                  expParams["dcmGitCommitId"],
                  expParams["numNodes"],
                  expParams["startTimeCutOff"],
                  expParams["percentageOfNodesToScoreValue"],
                  expParams["timeScaleDown"],
                  expParams["scenario"],
                  expParams["affinityProportion"]))

    trace = glob.glob(trace + "/trace")
    if (len(trace) > 0):
        with open(trace[0]) as traceFile:
            batchId = 1
            metrics = {}
            metrics["scopeLatency"] = 0 # when scope is not used
            variablesBeforePresolve = True
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

                if ("scope fraction" in line):
                    split = line.split()
                    fraction = split[-1]
                    conn.execute("insert into scope_fraction values (?, ?, ?)",
                                 (expParams["expId"], batchId, float(fraction)))

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

                if ("Scope filtering" in line):
                    split = line.split()
                    latency = split[-1][:-3] # Remove ns and period
                    metrics["scopeLatency"] = latency

                if ("Solver has run successfully" in line):
                    split = line.split()
                    latency = split[10][:-3] # Remove ns and period
                    metrics["dcmSolveTime"] = latency
                    conn.execute("insert into dcm_metrics values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                 (expParams["expId"],
                                  batchId,
                                  metrics["dcmSolveTime"],
                                  metrics["fetchcount"],
                                  metrics["scopeLatency"],
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

                if ("Adding pod" in line):
                    split = line.split("Adding pod")[-1].split()
                    pod = split[0]
                    uid = split[2].split(',')[0]
                    event = "ADD"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Received stale event for pod that we already deleted:" in line):
                    split = line.split("Received stale event for pod that we already deleted")[-1].split()
                    pod = split[0]
                    uid = split[2].split(',')[0]
                    event = "IGNORE_ON_ADD_ALREADY_DELETED"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Deleting pod" in line):
                    split = line.split("Deleting pod")[-1].split()
                    pod = split[0]
                    uid = split[2].split(',')[0]
                    event = "DELETE"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("does not exist. Skipping" in line):
                    split = line.split("does not exist. Skipping")[0].split()
                    pod = split[-3]
                    uid = split[-1].split(')')[0]
                    event = "IGNORE_ON_UPDATE_NO_EXIST"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Received a stale pod event" in line):
                    split = line.split("Received a stale pod event")[-1].split()
                    pod = split[0]
                    uid = split[2].split(',')[0]
                    event = "IGNORE_ON_UPDATE_STALE"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Received a duplicate event for a node that we have already scheduled" in line):
                    split = line.split("have already scheduled")[-1].split()
                    pod = split[3].split(')')[0]
                    uid = ""
                    event = "IGNORE_ON_UPDATE_DUPLICATE"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Received stale event for pod that we already deleted:" in line):
                    split = line.split("we already deleted")[-1].split()
                    pod = split[0]
                    uid = split[2].split(',')[0]
                    event = "IGNORE_ON_UPDATE_ALREADY_DELETED"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Updating pod" in line):
                    split = line.split("Updating pod")[-1].split()
                    pod = split[0]
                    uid = split[2].split(',')[0]
                    event = "UPDATE"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Insert/Update pod" in line):
                    split = line.split("Insert/Update pod")[-1].split()
                    pod = split[1]
                    uid = split[0].split(',')[0]
                    event = "INSERT/UPDATE"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Attempting to bind" in line):
                    split = line.split(":")[-1].split()
                    pod = split[0]
                    uid = ""
                    event = "BIND_ATTEMPT"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Binding" in line):
                    split = line.split("pod:")[-2].split()
                    pod = split[0]
                    uid = split[2].split(')')[0]
                    event = "BIND"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Attempting to delete" in line):
                    split = line.split(":")[-1].split()
                    pod = split[0]
                    uid = ""
                    event = "DELETE_ATTEMPT"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Delete" in line):
                    split = line.split("pod:")[-2].split()
                    pod = split[0]
                    uid = split[2].split(')')[0]
                    event = "DELETE"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("Attempting Preemption" in line):
                    split = line.split("pod:")[-1].split()
                    pod = split[0]
                    uid = ""
                    event = "PREEMPT_ATTEMPT"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("will be preempted" in line):
                    split = line.split("pod:")[-1].split()
                    pod = split[0]
                    uid = ""
                    event = "PREEMPT"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))

                if ("could not be assigned a node even with preemption" in line):
                    split = line.split("pod:")[-1].split()
                    pod = split[0]
                    uid = ""
                    event = "PREEMPT_FAIL"
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))



                if ("Scheduling decision for pod" in line):
                    split = line.split("Scheduling decision for pod")[-1].split()
                    pod = split[0]
                    batch = split[5]
                    bindTime = split[-1]
                    event = "SCHEDULE"
                    conn.execute("insert into scheduler_trace values (?, ?, ?, ?)",
                                 (expParams["expId"], pod, bindTime, batch))
                    conn.execute("insert into pod_events values (?, ?, ?, ?, ?)",
                                 (expParams["expId"], batchId, pod, uid, event))
                    batchId = int(batch) + 1

    conn.commit()
