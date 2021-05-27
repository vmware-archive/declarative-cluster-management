# DCM Reference

To get an overview of DCM, we recommend starting with the [tutorial](tutorial.md). It will introduce the basics
of DCM's models, specifying schemas with variables, and constraints that should apply against the variables.

This document lays out all of DCM's APIs to instantiate models and specify constraints using SQL. 

* [Model API](#model-api)  
    * [Instantiating models with Model.build()](#instantiating-a-model)  
    * [Solving models](#solving-models)
      * [model.updateData()](#modelupdatedata)    
      * [model.solve()](#modelsolve)
    * [Debugging models](#finding-out-which-constraints-were-unsatisfiable)
* [Writing constraints](#writing-constraints)
   * [Variable columns](#variable-columns)
   * [Hard constraints](#hard-constraints)  
   * [Soft constraints](#soft-constraints)  
* [Supported column types for inputs](#supported-column-types-for-inputs)
* [Boolean expressions](#boolean-expressions)
* [Arithmetic operators](#arithmetic-operators)
* [Supported aggregates](#supported-aggregates)


## Model API
The APIs below are described in [Model API Javadoc](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html).

### Instantiating models with Model.build()

There are two methods to build Models.

[Model.build(DSLContext conn, List\<String\> constraints)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#build(org.jooq.DSLContext,java.util.List))  
[Model.build(DSLContext conn, ISolverBackend solverBackend, List\<String\> constraints)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#build(org.jooq.DSLContext,com.vmware.dcm.backend.ISolverBackend,java.util.List))


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
                                               .setPrintDiagnostics(debugMode)
                                               .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
          return Model.build(conn, orToolsSolver, policies);
      case "MNZ-CHUFFED":
          final File modelFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_model.mzn");
          final File dataFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_data.dzn");
          final MinizincSolver solver = new MinizincSolver(modelFile, dataFile, new Conf());
          return Model.build(conn, solver, policies);
      default:
          throw new IllegalArgumentException(solverToUse);
  ```
  To see all the configuration parameters for an `OrToolsSolver` instance, see the 
[OrToolsSolverBuilder Javadocs](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/backend/ortools/OrToolsSolver.Builder.html). 

### Solving models

Once a model is instantiated using `Model.build()`, the returned model needs to be synchronized with
the database using `model.updateData()` to gather inputs and then solved using `model.solve()`. 

#### Fetch inputs 

  There are two methods to retrieve the latest records from the database to be used as inputs
for the solver.

  [Model.updateData()](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#updateData())  
  [Model.updateData(Function\<Table\<?\>, Result\<? extends Record\>\> fetcher)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#updateData(java.util.function.Function))

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

#### Compute a solution

There are two methods to solve models based on the most recent inputs fetched via `model.updateData()`.

[Model.solve(String tableName)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#solve())  
[Model.solve(Set\<String\> tableNames)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#solve(java.util.Set))

Both methods return records corresponding to one or more tables (specified by the `tableName/tableNames` argument).
If the call to `solve()` succeeds, tables with variable columns will have their values updated as per the 
constraints specified during `Model.build()`. If `solve()` fails, a `SolverException` exception is thrown.

### Finding out which constraints were unsatisfiable

If `model.solve()` fails, a `SolverException` is thrown. If the model was proven to be unsatisfiable,
the or-tools solver will also compute the set of offending constraints. This set can be accessed
via the `SolverException.core()` method. For now, `core()` returns only the string names of constraint
views that were unsatisfiable. We are currently working on returning fine-grained information about
which table-rows contributed to the unsatisfiability. 

<!-- embedme ../dcm/src/test/java/com/vmware/dcm/ModelTest.java#L238-L263 -->
```java
final DSLContext conn = DSL.using("jdbc:h2:mem:");
conn.execute("create table t1(id integer, controllable__var integer)");
conn.execute("insert into t1 values (1, null)");
conn.execute("insert into t1 values (2, null)");
conn.execute("insert into t1 values (3, null)");

// Unsatisfiable
final String allDifferent = "create view constraint_all_different as " +
        "select * from t1 check all_different(controllable__var) = true";

// Unsatisfiable
final String domain1 = "create view constraint_domain_1 as " +
        "select * from t1 check controllable__var >= 1 and controllable__var <= 2";

// Satisfiable
final String domain2 = "create view constraint_domain_2 as " +
        "select * from t1 check id != 1 or controllable__var = 1";

final Model model = Model.build(conn, List.of(allDifferent, domain1, domain2));
model.updateData();
try {
    model.solve("T1");
    fail();
} catch (final SolverException exception) {
    assertTrue(exception.core().containsAll(List.of("constraint_all_different", "constraint_domain_1")));
}
```

## Writing constraints

### Variable columns

Every DCM model computes values for one or more *variable columns*. A variable column is a column whose name
is prefixed with the keyword `controllable__`. A variable column can be of integer, bigint or
varchar types. DCM guarantees that a returned solution will assign values to variable columns such that
all hard constraints are satisfied, while maximizing  the number of soft constraints that are met.

A variable column can appear in a table or a view.

Example usage in a table:
<!-- embedme ../examples/src/main/resources/schema.sql#L11-L18 -->
```sql
-- controllable__physical_machine represents a variable that the solver will assign values to
create table virtual_machine (
    name varchar(30) primary key not null,
    cpu  integer  not null,
    memory integer  not null,
    controllable__physical_machine varchar(30),
    foreign key (controllable__physical_machine) references physical_machine(name)
);
```

Example usage in a view:
<!-- embedme ../k8s-scheduler/src/main/resources/scheduler_tables.sql#L213-L232 -->
```sql
create view pods_to_assign_no_limit as
select
  uid,
  pod_name,
  status,
  node_name as controllable__node_name,
  namespace,
  cpu_request,
  memory_request,
  ephemeral_storage_request,
  pods_request,
  owner_name,
  creation_timestamp,
  has_node_selector_labels,
  has_pod_affinity_requirements,
  has_pod_anti_affinity_requirements,
  equivalence_class,
  qos_class
from pod_info
where status = 'Pending' and node_name is null and schedulerName = 'dcm-scheduler';
```

### Hard constraints

A hard constraint is structured as a view that specifies a relation followed by a `CHECK` clause. The `CHECK` clause
specifies a predicate that must hold true for every record produced by the relation. For example,

```sql
create view constraint_simple as
select * from virtual_machine
check controllable__physical_machine = 'pm3';
```

### Soft constraints

A soft constraint is structured as a view that computes a single column of integers, 
whose value DCM will try to maximize, followed by the `maximize` annotation. For example,

```sql
create view objective_load_cpu as 
select min(cpu_spare) from spare_cpu
maximize;
```

or

```sql
create view objective_load_cpu as 
select cpu_spare from spare_cpu
maximize;
```

DCM will try to maximize the sum of all objective functions. An objective function view that computes a 
column expression (like the second example above) is treated as one objective function per cell in the column.

### Supported column types for inputs

#### integer
#### bigint
#### varchar
#### array


### Boolean expressions

In the below table: a `Numeric expr` may be an `integer` column or literal, a `bigint` column or literal, 
or an arithmetic expression. `T expr` is an expression of type T.

Name | Operator | Arguments | Example
--- | --- | --- | ---
Boolean AND | `AND`| `(boolean expr) AND (boolean expr)`| `CHECK (column_a = 10 AND column_b < 100)` 
Boolean OR | `OR` | `(boolean expr) OR (boolean expr)`| `CHECK (column_a = 10 OR column_b < 100)`
Equals | `=` | `(T expr) = (T expr)`| `CHECK (column_a = 10)`
Not Equals | `!=` | `(T expr) = (T expr)`| `CHECK (column_a != 10)`
Greater than | `\>` | `(Numeric expr) > (Numeric expr)`| `CHECK (column_a > 10)`
Greater than or equal to | `\>=` | `(Numeric expr) \>= (Numeric expr)`| `CHECK (column_a \>= 10)`
Less than | `\<` | `(Numeric expr) \< (Numeric expr)`| `CHECK (column_a \< 10)`
Less than or equal to | `\<=` | `(Numeric expr) \<= (Numeric expr)`| `CHECK (column_a \<= 10)`
In | `IN` | `(T expr) IN (SELECT T expr FROM....)`| `CHECK (column_a IN (SELECT column_b FROM mytable))`
Exists | `EXISTS` | `EXISTS (SELECT T expr FROM...)`| `CHECK EXISTS (SELECT column_a = 10 FROM...)`
Array Contains | `CONTAINS` | `CONTAINS (ARRAY expr, ARRAY column)`| `CHECK CONTAINS (column_arr, controllable__a)`

### Arithmetic operators

Name | Operator | Arguments | Example
--- | --- | --- | ---
Plus | `+`| `(Numeric expr) + (Numeric expr)`| `CHECK (column_a + column_b = 10)`
Minus | `-`| `(Numeric expr) - (Numeric expr)`| `CHECK (column_a - column_b = 10)`
Multiplication | `*`| `(Numeric expr) * (Numeric expr)`| `CHECK (column_a * column_b = 10)`
Integer division | `/`| `(Numeric expr) / (Numeric expr)`| `CHECK (column_a / column_b = 10)`
Modulus | `%`| `(Numeric expr) % (Numeric expr)`| `CHECK (column_a % column_b = 10)`

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
