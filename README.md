[![Build Status](https://circleci.com/gh/vmware/declarative-cluster-management.svg?style=svg)](https://circleci.com/gh/vmware/declarative-cluster-management)
[![codecov](https://codecov.io/gh/vmware/declarative-cluster-management/branch/master/graph/badge.svg)](https://codecov.io/gh/vmware/declarative-cluster-management)

# Declarative Cluster Management

## Overview

Modern cluster management systems like Kubernetes routinely grapple
with hard combinatorial optimization problems: load balancing,
placement, scheduling, and configuration. Implementing application-specific algorithms to
solve these problems is notoriously hard to do, making it challenging to evolve the system over time 
and add new features. DCM is tool to overcome this challenge. It enables programmers to build schedulers 
and cluster managers using a high-level declarative language (SQL). 

With DCM, building a scheduler involves representing cluster state in an SQL database, and writing constraints
and policies that should apply on that state using SQL. Behind-the-scenes, the DCM compiler and runtime generates an 
encoding of the constraints into an optimization model, which it solves using an off-the-shelf solver. 

### References

To learn more about DCM, we suggest going through the following research papers:

* [Automating Cluster Management with Weave](https://arxiv.org/pdf/1909.03130.pdf)<br>
  Lalith Suresh, Joao Loff, Faria Kalim, Nina Narodytska, Leonid Ryzhyk, Sahan Gamage, Brian Oki, Zeeshan Lokhandwala, Mukesh Hira, Mooly Sagiv. arXiv preprint arXiv:1909.03130 (2019).

* [Synthesizing Cluster Management Code for Distributed Systems](https://dl.acm.org/citation.cfm?id=3321444)<br>
  Lalith Suresh, João Loff, Nina Narodytska, Leonid Ryzhyk, Mooly Sagiv, and Brian Oki. In Proceedings of the Workshop on Hot Topics in Operating Systems (HotOS 2019).
  ACM, New York, NY, USA, 45-50. DOI: https://doi.org/10.1145/3317550.3321444

## Try it out

### Pre-requisites

1. Maven and JDK 12 for building.

2. We test regularly on OSX and Ubuntu 18.04.

3. We currently support two solver backends. Make sure to install both of them to run the build: 

   * **MiniZinc (version 2.3.2)**. You can download it from: [https://www.minizinc.org/software.html](https://www.minizinc.org/software.html)

     Make sure you are able to invoke the `minizinc` binary from your commandline.

   * **Google OR-tools CP-SAT (version 7.4)**. To install, download the binary package for your platform from: [https://github.com/google/or-tools/releases/tag/v7.4](https://github.com/google/or-tools/releases/tag/v7.4)

     Untar the downloaded bundle and run the following command in the `<or-tools>/lib/` folder to install the or-tools jar file:

     ```
      $: mvn install:install-file -Dfile=com.google.ortools.jar -DgroupId=com.google -DartifactId=ortools -Dversion=7.4 -Dpackaging=jar
     ```

     Next, set up the following environment variable to point to the or-tools shared library:

     On OSX:
     ```
      export OR_TOOLS_LIB=<or-tools>/lib/libjniortools.jnilib
     ```

     On Linux:
     ```
      export OR_TOOLS_LIB=<or-tools>/lib/libjniortools.so
     
     ```

### Building

We use maven as our build system. You can run the following command once you've set up the solvers as listed above:

```
 $: mvn package
```

## How do I use DCM?

We suggest reading going through use case shown in the `examples/` folder, where we demonstrate a
simple cluster manager.

- Let's start with `examples/src/main/java/org/dcm/examples/LoadBalance.java`, a simple example to illustrate
  how one would load balance VMs across physical machines using DCM.
  
- The first step to using DCM is to set up a database connection, and instantiate a model using
  `Model.buildModel()`. The `setup()` method returns a [JOOQ](https://github.com/jooq/) connection to an 
  initialized, in-memory database for our example. In a few steps, we will see what kind 
  of constraints we can pass to the model: 

   ```java
    LoadBalance(final List<String> constraints) {
        conn = setup();
        model = Model.buildModel(conn, constraints);
    }
   ```

  The database returned by the `setup()` method is initialized with the following schema, representing a 
  table each for physical and virtual machines:

   ```sql
    create table physical_machine (
        name varchar(30) primary key,
        cpu_capacity integer,
        memory_capacity integer
    );
    
    -- controllable__physical_machine represents a variable that the solver will assign values to
    create table virtual_machine (
        name varchar(30) primary key not null,
        cpu  integer  not null,
        memory integer  not null,
        controllable__physical_machine varchar(30),
        foreign key (controllable__physical_machine) references physical_machine(name)
    );
  ```
  Here, the `virtual_machine` table has a special column, `controllable__physical_machine`. We expect DCM to
  assign values to this column to satisfy all the supplied constraints. In doing so, we expect DCM to help us
  find a mapping of virtual machines to physical machines. 

- We can update the tables with some simple helper methods, `LoadBalance.addVm()` and `LoadBalance.addNode()`. 

- Let's now drive our simple example with some tests. Have a look at `examples/src/test/java/org/dcm/examples/LoadBalanceTest.java`.
  All the tests populate the database with the following initial state using the `LoadBalanceTest.addInventory()` method:
  
  ```
  +----+------------+---------------+
  |NAME|CPU_CAPACITY|MEMORY_CAPACITY|
  +----+------------+---------------+
  |pm0 |          50|             50|
  |pm1 |          50|             50|
  |pm2 |          50|             50|
  |pm3 |          50|             50|
  |pm4 |          50|             50|
  +----+------------+---------------+
  
  +----+----+------+------------------------------+
  |NAME| CPU|MEMORY|CONTROLLABLE__PHYSICAL_MACHINE|
  +----+----+------+------------------------------+
  |vm0 |  10|    10|{null}                        |
  |vm1 |  10|    10|{null}                        |
  |vm2 |  10|    10|{null}                        |
  |vm3 |  10|    10|{null}                        |
  |vm4 |  10|    10|{null}                        |
  |vm5 |  10|    10|{null}                        |
  |vm6 |  10|    10|{null}                        |
  |vm7 |  10|    10|{null}                        |
  |vm8 |  10|    10|{null}                        |
  |vm9 |  10|    10|{null}                        |
  +----+----+------+------------------------------+
  ```
 
- Let's first start by specifying a very simple constraint: assign all VMs to physical machine `pm3`:

  ```java
    /*
     * A simple constraint that forces all assignments to go the same node
     */
    @Test
    public void testSimpleConstraint() {
        final String allVmsGoToPm3 = "create view constraint_simple as " +
                                     "select * from virtual_machine " +
                                     "where controllable__physical_machine = 'pm3'";
        final LoadBalance lb = new LoadBalance(Collections.singletonList(allVmsGoToPm3));
        addInventory(lb);
        final Result<? extends Record> results = lb.run();
        System.out.println(results);
        results.forEach(e -> assertEquals(e.get("CONTROLLABLE__PHYSICAL_MACHINE"), "pm3"));
    }
  ```
  To specify a constraint, we simply create a view whose name is prefixed with `constraint_`. The where clause
  specifies a predicate that should hold true for all records returned by the relation being selected (in this case,
  the table `virtual_machine`). In this case, the constraint requires all VMs to be assigned to `pm3`, which yields
  the following result:
  ```
  +----+----+------+------------------------------+
  |NAME| CPU|MEMORY|CONTROLLABLE__PHYSICAL_MACHINE|
  +----+----+------+------------------------------+
  |vm0 |  10|    10|pm3                           |
  |vm1 |  10|    10|pm3                           |
  |vm2 |  10|    10|pm3                           |
  |vm3 |  10|    10|pm3                           |
  |vm4 |  10|    10|pm3                           |
  |vm5 |  10|    10|pm3                           |
  |vm6 |  10|    10|pm3                           |
  |vm7 |  10|    10|pm3                           |
  |vm8 |  10|    10|pm3                           |
  |vm9 |  10|    10|pm3                           |
  +----+----+------+------------------------------+
  ```

- Now let's get serious and add a capacity constraint. On any given physical machine, we want to ensure that the
  sum of demands from all VMs assigned to that machine does not exceed the capacity of that machine. To specify that,
  we can try the following constraint:
  
  ```java
      /*
       * We now add a capacity constraint to make sure that no physical machine is assigned more VMs
       * than it has capacity for. Given the constants we've chosen in addInventory(), there should be
       * at least two physical machines that receive VMs.
       */
      @Test
      public void testCapacityConstraints() {
          final String capacityConstraint =
                  "create view constraint_capacity as " +
                  "select * from virtual_machine " +
                  "join physical_machine " +
                  "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                  "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity " +
                  "having sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
                  "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";
  
          final LoadBalance lb = new LoadBalance(Collections.singletonList(capacityConstraint));
          addInventory(lb);
          final Result<? extends Record> results = lb.run();
          System.out.println(results);
          final Set<String> setOfPhysicalMachines = results.stream()
                                                       .map(e -> e.get("CONTROLLABLE__PHYSICAL_MACHINE", String.class))
                                                       .collect(Collectors.toSet());
          System.out.println(result);
          assertTrue(setOfPhysicalMachines.size() >= 2);
      }
  ```
  Here, the `having` clause specifies the capacity constraint. On my machine, this assigns all VMs evenly
  between physical machines `pm2` and `pm4`:
  ```
    +----+----+------+------------------------------+
    |NAME| CPU|MEMORY|CONTROLLABLE__PHYSICAL_MACHINE|
    +----+----+------+------------------------------+
    |vm0 |  10|    10|pm2                           |
    |vm1 |  10|    10|pm2                           |
    |vm2 |  10|    10|pm2                           |
    |vm3 |  10|    10|pm2                           |
    |vm4 |  10|    10|pm2                           |
    |vm5 |  10|    10|pm4                           |
    |vm6 |  10|    10|pm4                           |
    |vm7 |  10|    10|pm4                           |
    |vm8 |  10|    10|pm4                           |
    |vm9 |  10|    10|pm4                           |
    +----+----+------+------------------------------+
  ```  
   
- Note that the constraints we have seen so far are hard constraints. Let's now add a soft constraint, a load balancing
  objective:
  ```java
      /*
       * Add a load balancing objective function. This should spread out VMs across all physical machines.
       */
      @Test
      public void testDistributeLoad() {
          final String capacityConstraint =
                  "create view constraint_capacity as " +
                  "select * from virtual_machine " +
                  "join physical_machine " +
                  "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                  "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity " +
                  "having sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
                  "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";
  
          final String spareCpu = "create view spare_cpu as " +
                  "select physical_machine.cpu_capacity - sum(virtual_machine.cpu) as cpu_spare " +
                  "from virtual_machine " +
                  "join physical_machine " +
                  "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                  "group by physical_machine.name, physical_machine.cpu_capacity";
  
          // Queries presented as objectives, will have their values maximized.
          final String distributeLoadCpu = "create view objective_load_cpu as select min(cpu_spare) from spare_cpu";
  
          final LoadBalance lb =
                    new LoadBalance(List.of(capacityConstraint, spareCpu, distributeLoadCpu));
          addInventory(lb);
          final Result<? extends Record> result = lb.run();
          final Set<String> setOfPhysicalMachines = result.stream()
                  .map(e -> e.get("CONTROLLABLE__PHYSICAL_MACHINE", String.class))
                  .collect(Collectors.toSet());
          System.out.println(result);
          assertEquals(NUM_PHYSICAL_MACHINES, setOfPhysicalMachines.size());
      }
  ```  
  
  In the above case, we borrow the capacity constraint from the previous example. We then create an intermediate
  view that computes the spare CPU capacity on each physical machine. We then state an objective function, which 
  are specified as SQL queries that return a single scalar value --- DCM will try to find assignments that maximize 
  the value of that query. In this case, we maximize the minimum spare CPU capacity, which has the intended load
  balancing effect. The result should therefore print something like:
  
  ```
    +----+----+------+------------------------------+
    |NAME| CPU|MEMORY|CONTROLLABLE__PHYSICAL_MACHINE|
    +----+----+------+------------------------------+
    |vm0 |  10|    10|pm4                           |
    |vm1 |  10|    10|pm0                           |
    |vm2 |  10|    10|pm0                           |
    |vm3 |  10|    10|pm2                           |
    |vm4 |  10|    10|pm4                           |
    |vm5 |  10|    10|pm3                           |
    |vm6 |  10|    10|pm3                           |
    |vm7 |  10|    10|pm2                           |
    |vm8 |  10|    10|pm1                           |
    |vm9 |  10|    10|pm1                           |
    +----+----+------+------------------------------+
  ``` 



## Information for developers

The entire build including unit tests can be triggered from the root folder with:
```bash
$: mvn package 
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
$: KUBECONFIG=~/.kube/kind-config-dcm-it mvn integration-test
```

To run a specific integration test class (example: `SchedulerIT` from the `k8s-scheduler` module):

```bash
$: KUBECONFIG=~/.kube/kind-config-dcm-it mvn integration-test -Dtest=SchedulerIT -DfailIfNoTests=false
```

Note, the `-DfailIfNoTests=false` flag is important, or the build will fail earlier modules that don't have tests
with the same class name.
