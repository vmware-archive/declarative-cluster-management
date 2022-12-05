[![License](https://img.shields.io/badge/License-BSD%202--Clause-green.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![Build Status](https://circleci.com/gh/vmware/declarative-cluster-management.svg?style=shield)](https://circleci.com/gh/vmware/declarative-cluster-management)
[![codecov](https://codecov.io/gh/vmware/declarative-cluster-management/branch/master/graph/badge.svg)](https://codecov.io/gh/vmware/declarative-cluster-management)
[![Maven Central](https://img.shields.io/maven-central/v/com.vmware.dcm/dcm.svg?color=green)](https://search.maven.org/search?q=g:%22com.vmware.dcm%22%20AND%20a:%22dcm%22)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/vmware/declarative-cluster-management)
[![javadoc](https://javadoc.io/badge2/com.vmware.dcm/dcm/javadoc.svg)](https://javadoc.io/doc/com.vmware.dcm/dcm)

## Declarative Cluster Management

1. [Overview](#overview)
2. [Download](#download)  
3. [Pre-requisites for use](#pre-requisites-for-use)
4. [VLDB23 Experiment Setup](#experiment)
5. [Documentation](#documentation)


### Overview

Modern cluster management systems like Kubernetes routinely grapple
with hard combinatorial optimization problems: load balancing,
placement, scheduling, and configuration. Implementing application-specific algorithms to
solve these problems is notoriously hard to do, making it challenging to evolve the system over time 
and add new features. 

DCM is a tool to overcome this challenge. It enables programmers to build schedulers 
and cluster managers using a high-level declarative language (C-SQL). 

Specifically, developers need to represent cluster state in an SQL database, and write constraints
and policies that should apply on that state using C-SQL. From the C-SQL specification, the DCM compiler synthesizes a 
program that at runtime, can be invoked to compute policy-compliant cluster management decisions given the latest 
cluster state.  Under the covers, the generated program efficiently encodes the cluster state as an 
optimization problem  that can be solved using off-the-shelf solvers, freeing developers from having to 
design ad-hoc heuristics.

The high-level architecture is shown in the diagram below.

<p align="center">
  <img src="https://github.com/vmware/declarative-cluster-management/blob/master/docs/arch_detailed.png"/>
</p>

### Download

The DCM project's groupId is `com.vmware.dcm` and its artifactId is `dcm`.
We make DCM's artifacts available through Maven Central.

To use DCM from a Maven-based project, use the following dependency:

```xml
<dependency>
    <groupId>com.vmware.dcm</groupId>
    <artifactId>dcm</artifactId>
    <version>0.13.0</version>
</dependency>
```

To use within a Gradle-based project:

```
implementation 'com.vmware.dcm:dcm:0.13.0'
```

### Pre-requisites for use

1. We test regularly on JDK 11 and 16.

2. We test regularly on OSX and Ubuntu 20.04.

3. We currently support two solver backends. 

   * **Google OR-tools CP-SAT (version 9.1.9490)**. This is available by default when using the maven dependency. 

   * **MiniZinc (version 2.3.2)**. This backend is currently being deprecated. If you still want to use it
     in your project, or if you want run all tests in this repository, you will have to install MiniZinc out-of-band. 
     
     To do so, download MiniZinc from [https://www.minizinc.org/software.html](https://www.minizinc.org/software.html)
   ... and make sure you are able to invoke the `minizinc` binary from your commandline.


### Documentation

The [Model](dcm/src/main/java/com/vmware/dcm/Model.java) class serves as DCM's public API. It exposes
two methods: `Model.build()` and `model.solve()`. 

* Check out the [tutorial](docs/tutorial.md) to learn how to use DCM by building a simple VM load balancer
* Check out our [research papers](#learn-more) for the back story behind DCM
* The Model API [Javadocs](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html)


### VLDB'23 Experiment Setup
#### DDlog Setup 
Clone the DDlog repository and checkout the vldb23 branch:
```
$: git clone https://github.com/kexinrong/differential-datalog.git
$: cd differential-datalog/
$: git fetch --all
$: git checkout vldb23
```

Install DDlog's SQL-frontend:
```
$: cd sql
$: ./install-ddlog-jar.sh
$: mvn install
```

#### DCM Setup 
Clone the DCM repository and checkout the vldb23 branch:
```
$: git clone https://github.com/vmware/declarative-cluster-management.git
$: cd declarative-cluster-management/
$: git fetch --all
$: git checkout vldb23
$: ./gradlew clean build -x test
```
Download workload traces:
```
$: cd workload-generator/
$: ./getAzureTraces.sh
```

#### Run benchmarks
If everything is setup correctly, you should be able to run the following benchmark:
```
$: ./gradlew runBenchmark --args="-n 500 -f v2-cropped.txt -c 100 -m 200 -t 10 -s 10000 -p 0"
```
The full description of the benchmark parameters can be found in EmulatedCluster.java:
```
options.addRequiredOption("n", "numNodes", true,
        "Number of nodes in experiment");
options.addRequiredOption("f", "traceFile", true,
        "Trace file to use from k8s-scheduler/src/test/resources/");
options.addRequiredOption("c", "cpuScaleDown", true,
        "Factor by which to scale down CPU resource demands for pods");
options.addRequiredOption("m", "memScaleDown", true,
        "Factor by which to scale down Memory resource demands for pods");
options.addRequiredOption("t", "timeScaleDown", true,
        "Factor by which to scale down arrival rate for pods");
options.addRequiredOption("s", "startTimeCutOff", true,
        "N, where we replay first N seconds of the trace");
options.addOption("p", "proportion", true,
        "P, from 0 to 100, indicating the proportion of pods that have 
         affinity/antiaffinity requirements");
options.addOption("S", "scopeOn", false,
        "enable auto-scope in scheduler");
options.addOption("df", "ddlogFile", true,
        "specify which ddlog program.dl to use");
options.addOption("k", "limit", true,
        "parameter for setting top k in scope");
```
We have provided instructions to reproduce Figure 12 in the paper.  
```
$: cd artifact/
$: python gen_scope_bench.py -r [path to trace files]
```
This will generate a bash script scope_bench.sh that runs the DDlog-backed DCM with and without FP-pushdown for cluster size = {500, 5000, 50000} x F={0%, 50%, 100%}, 5 times per configuration. To run the script:  
```
$: chmod +x scope_bench.sh
$: cd ..
$: ./artifact/scope_bench.sh
```
To parse and visualize the generated traces files, please check out the notebook `artifact/plot_trace.ipynb`. By default, the notebook assumes the trace files are stored under `artifact/traces`. 