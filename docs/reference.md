# DCM Reference

To get an overview of DCM, we recommend starting with the [tutorial](tutorial.md). It will introduce the basics
of DCM's models, specifying schemas with variables, and constraints that should apply against the variables.

This document lays out all of DCM's APIs to instantiate models and specify constraints using SQL. 

* [Model API](#model-api)  
    * [Instantiating models with Model.build()](#instantiating-a-model)  
    * [Solving models](#solving-models)
      * [model.updateData()](#modelupdatedata)    
      * [model.solve()](#modelsolve)  
* [Writing constraints](#writing-constraints)  
   * [Hard constraints](#hard-constraints)  
   * [Soft constraints](#soft-constraints)  
* [Supported column types for inputs](#supported-column-types-for-inputs)
* [Boolean expressions](#boolean-expressions)
* [Arithmetic operators](#arithmetic-operators)
* [Supported aggregates](#supported-aggregates)


## Model API

The APIs below are described in [Model API Javadoc](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html).

### Instantiating models with Model.build()

There are two overloads available to build Models.
```java
Model.build(DSLContext conn, List<String> constraints)
Model.build(DSLContext conn, ISolverBackend solverBackend, List<String> constraints)
```

* The `conn` argument is a connection to a database, created using the `JOOQ` library. For example: 
  <!-- embedme ../examples/src/test/java/com/vmware/dcm/examples/QuickStartTest.java#L23-L23 -->
  ```java
  final DSLContext conn = DSL.using("jdbc:h2:mem:");
  ```

* The `constraints` argument is a list of views that collectively form the constraints to the model. Each `String`
is a single SQL view. See the section below on [writing constraints](#writing-constraints).

* The `solverBackend` argument supplies a configured solver to use under-the-covers. This allows a user to configure
the solver's properties (such as the timeout to use). If you omit the argument, an instance of the `OrToolsSolver` 
is used. Here's an example of this API's use in our Kubernetes scheduler:

  <!-- embedme ../k8s-scheduler/src/main/java/com/vmware/dcm/Scheduler.java#L178-L191 -->
  ```java
  switch (solverToUse) {
      case "ORTOOLS":
          final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                                               .setNumThreads(numThreads)
                                               .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
          return Model.build(conn, orToolsSolver, policies);
      case "MNZ-CHUFFED":
          final File modelFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_model.mzn");
          final File dataFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_data.dzn");
          final MinizincSolver solver = new MinizincSolver(modelFile, dataFile, new Conf());
          return Model.build(conn, solver, policies);
      default:
          throw new IllegalArgumentException(solverToUse);
  }
  ```
  To see all the configuration parameters for an `OrToolsSolver` instance, see the 
[OrToolsSolverBuilder Javadocs](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/backend/ortools/OrToolsSolver.Builder.html) 

### Solving models

Once a model is instantiated using `Model.build()`, the returned model needs to be synchronized with
the database using `model.updateData()` to gather inputs and then solved using `model.solve()`. 

#### model.updateData() 

There are two overloads available to retrieve the latest records from the database to be used as inputs
for the solver.

```java
Model.updateData()
Model.updateData(Function<Table<?>, Result<? extends Record>> fetcher)
```

The first method simply invokes `select * from <table>` for all tables and views in the constraints that need
to be fetched from the database.

The second overload allows users to exercise tighter control on how individual tables are fetched. For example,
a specific table might be best fetched using a cache or the user might want to dynamically subset which rows
are fetched from specific tables.

Here is a code sample illustrating how this API could be used:
  <!-- embedme ../dcm/src/test/java/com/vmware/dcm/ModelTest.java#L101-L109 -->
  ```java
  final int minimumPodId = 7;
  model.updateData((table) -> {
      if (table.getName().equalsIgnoreCase("pod")) {
          // Should only pull in the 2nd record
          return conn.selectFrom(table).where(field("pod_id").gt(minimumPodId)).fetch();
      }
      return conn.selectFrom(table).fetch();
  });
  final Result<? extends Record> result = model.solve("POD");
  ``` 

#### model.solve()

There are two overloads available to solve models based on the most recent inputs fetched via `model.updateData()`.

```java
Model.solve(String tableName)
Model.solve(Set<String> tableNames)
```

Both methods return records corresponding to one or more tables (specified by the `tableName/tableNames` argument).
If the call to `solve()` succeeds, tables with variable columns will have their values updated as per the 
constraints specified during `Model.build()`. If `solve()` fails, a `SolverException` exception is thrown.

## Writing constraints

Hard constraints

Soft constraints

### Supported column types for inputs

#### integer
#### bigint
#### varchar
#### array


### Boolean expressions

#### and

#### or

#### =

#### !=

#### \>

#### \>=

#### \<

#### \<=

#### in

#### exists

#### contains

### Arithmetic operators

### +
### -
### *
### /
### %

### Supported aggregates

#### sum

#### count

#### min

#### max

#### all_different

#### all_equal

#### increasing

#### capacity_constraint

#### contains
