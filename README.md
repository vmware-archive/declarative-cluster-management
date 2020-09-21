[![License](https://img.shields.io/badge/License-BSD%202--Clause-green.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![Build Status](https://circleci.com/gh/vmware/declarative-cluster-management.svg?style=svg)](https://circleci.com/gh/vmware/declarative-cluster-management)
[![codecov](https://codecov.io/gh/vmware/declarative-cluster-management/branch/master/graph/badge.svg)](https://codecov.io/gh/vmware/declarative-cluster-management)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/vmware/declarative-cluster-management)

## Declarative Cluster Management

1. [Overview](#overview)
2. [Download](#download)  
3. [Pre-requisites for use](#pre-requisites-for-use)
4. [DCM API and tutorial](#dcm-api)
5. [Information for developers](#information-for-developers)
6. [Learn more](#learn-more)

### Overview

Modern cluster management systems like Kubernetes routinely grapple
with hard combinatorial optimization problems: load balancing,
placement, scheduling, and configuration. Implementing application-specific algorithms to
solve these problems is notoriously hard to do, making it challenging to evolve the system over time 
and add new features. DCM is a tool to overcome this challenge. It enables programmers to build schedulers 
and cluster managers using a high-level declarative language (SQL). 

With DCM, building a scheduler involves representing cluster state in an SQL database, and writing constraints
and policies that should apply on that state using SQL. Behind-the-scenes, the DCM compiler and runtime generates an 
encoding of the constraints into an optimization model, which it solves using an off-the-shelf solver. 


### Download

The DCM project's groupId is `com.vmware.dcm`, and the artifactId for the DCM library is `dcm`.
We make DCM's artifacts available through Maven Central.

To use DCM from a Maven-based project, use the following dependency:

```
<dependency>
    <groupId>com.vmware.dcm</groupId>
    <artifactId>dcm</artifactId>
    <version>0.2.0</version>
</dependency>
```

To use within a Gradle-based project:

```
implementation 'com.vmware.dcm:dcm:0.2.0'
```

For now, one of DCM's dependencies is only available on Jcenter (we expect this to change soon). 
Until then, you also need to add Jcenter to your list of repositories.

Maven:
```
<repositories>
    <repository>
      <id>jcenter</id>
      <url>https://jcenter.bintray.com/</url>
    </repository>
</repositories>
```

Gradle:
```
repositories {
    jcenter()
}
```

### Pre-requisites for use

1. We build the repository using JDK 12. Please file a Pull Request if you'd like the artifacts to be built for
   an older JDK.

2. We test regularly on OSX and Ubuntu 18.04.

3. We currently support two solver backends. 

   * **Google OR-tools CP-SAT (version 7.8)**. This is available by default when using the maven dependency. 

   * **MiniZinc (version 2.3.2)**. This backend is currently being deprecated. If you still want to use it,
   you will have to install MiniZinc out-of-band. 
   
   To do so, download MiniZinc from [https://www.minizinc.org/software.html](https://www.minizinc.org/software.html)
   ... and make sure you are able to invoke the `minizinc` binary from your commandline.


### DCM API and tutorial

The [Model](dcm/src/main/java/com/vmware/dcm/Model.java) class serves as DCM's public API. It exposes
three methods: `Model.build()`, `model.updateData()` and `model.solve()`. We encourage you
to check out the [tutorial](docs/tutorial.md) to learn how to use this API.

### Information for developers

The entire build including unit tests can be triggered from the root folder with the following command (make
sure to setup both solvers first):

```bash
$: ./gradlew build
```

The Kubernetes scheduler also comes with integration tests that run against a real Kubernetes cluster. 
*It goes without saying that you should not point to a production cluster as these tests repeatedly delete all 
running pods and deployments*. To run these integration-tests, make sure you have a valid `KUBECONFIG`
environment variable that points to a Kubernetes cluster. 

We recommend setting up a local multi-node cluster and  a corresponding `KUBECONFIG` using 
[kind](https://kind.sigs.k8s.io/docs/user/quick-start/). Once you've installed `kind`, run the following
to create a test cluster:
 
```bash
 $: kind create cluster --config k8s-scheduler/src/test/resources/kind-test-cluster-configuration.yaml --name dcm-it
```

The above step will create a configuration file in your home folder (`~/.kube/kind-config-dcm-it`), make sure
you initialize a `KUBECONFIG` environment variable to point to that path. 
 
You can then execute the following command to run integration-tests against the created local cluster:

```bash
$: KUBECONFIG=~/.kube/kind-config-dcm-it ./gradlew :k8s-scheduler:integrationTest
```

To run a specific integration test class (example: `SchedulerIT` from the `k8s-scheduler` module):

```bash
$: KUBECONFIG=~/.kube/kind-config-dcm-it ./gradlew :k8s-scheduler:integrationTest --tests SchedulerIT
```


### Learn more

To learn more about DCM, we suggest going through the following research papers:

* [Building Scalable and Flexible Cluster Managers Using Declarative Programming](https://www.usenix.org/conference/osdi20/accepted-papers) <br>
  Lalith Suresh, Joao Loff, Faria Kalim, Sangeetha Abdu Jyothi, Nina Narodytska, Leonid Ryzhyk, Sahan Gamage, Brian Oki, Pranshu Jain, Michael Gasch. 
  To appear, 14th USENIX Symposium on Operating Systems Design and Implementation, (OSDI 2020).

* [Automating Cluster Management with Weave](https://arxiv.org/pdf/1909.03130.pdf)<br>
  Lalith Suresh, Joao Loff, Faria Kalim, Nina Narodytska, Leonid Ryzhyk, Sahan Gamage, Brian Oki, Zeeshan Lokhandwala, Mukesh Hira, Mooly Sagiv. arXiv preprint arXiv:1909.03130 (2019).

* [Synthesizing Cluster Management Code for Distributed Systems](https://dl.acm.org/citation.cfm?id=3321444)<br>
  Lalith Suresh, João Loff, Nina Narodytska, Leonid Ryzhyk, Mooly Sagiv, and Brian Oki. In Proceedings of the Workshop on Hot Topics in Operating Systems (HotOS 2019).
  ACM, New York, NY, USA, 45-50. DOI: https://doi.org/10.1145/3317550.3321444
