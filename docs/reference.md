# DCM Reference

To get an overview of DCM, we recommend starting with the [tutorial](tutorial.md). It will introduce the basics
of DCM's models, specifying schemas with variables, and constraints that should apply against the variables.

This document lays out all of DCM's APIs to instantiate models and specify constraints using SQL. 

* [Model API](#model-api)  
    * [Instantiating models with Model.build()](#instantiating-a-model)  
    * [Solving models](#solving-models)
      * [model.solve()](#compute-a-solution)
      * [Debugging models](#finding-out-which-constraints-were-unsatisfiable)
* [Writing constraints](#writing-constraints)
   * [Variable columns](#variable-columns)
   * [Hard constraints](#hard-constraints)  
   * [Objective functions](#objective-functions)  
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

  <!-- embedme ../k8s-scheduler/src/main/java/com/vmware/dcm/Scheduler.java#L109-L112 -->
  ```java
  final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
          .setNumThreads(numThreads)
          .setPrintDiagnostics(debugMode)
          .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
  ```
  To see all the configuration parameters for an `OrToolsSolver` instance, see the 
[OrToolsSolverBuilder Javadocs](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/backend/ortools/OrToolsSolver.Builder.html). 

### Solving models

Once a model is instantiated using `Model.build()`, the returned model can be solved using `model.solve()`. 

#### Compute a solution

There are two methods to solve models based on the current state of the database. That is, the inputs for the
model are fetched by invoking `select * from <table>` for all tables and views in the constraints that need
to be fetched from the database. 

[Model.solve(String tableName)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#solve())  
[Model.solve(Set\<String\> tableNames)](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#solve(java.util.Set))

Both methods return records corresponding to one or more tables (specified by the `tableName/tableNames` argument).
If the call to `solve()` succeeds, tables with variable columns will have their values updated as per the 
constraints specified during `Model.build()`. If `solve()` fails, a `SolverException` exception is thrown.

In addition, there are two methods to solve models based on a custom `fetcher`. These methods
allow users to exercise tighter control on how individual tables are fetched.

[Model.solve(String tableName, Function\<Table\<?\>, Result\<? extends Record\>\> fetcher))](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#solve())  
[Model.solve(Set\<String\> tableNames, Function\<Table\<?\>, Result\<? extends Record\>\> fetcher))](https://javadoc.io/doc/com.vmware.dcm/dcm/latest/com/vmware/dcm/Model.html#solve(java.util.Set))

For example,  a specific table might be best fetched using a cache or the user might want to dynamically subset which 
rows  are fetched from specific tables. Here is a code sample illustrating how this API could be used:

  <!-- embedme ../dcm/src/test/java/com/vmware/dcm/ModelTest.java#L104-L111 -->
  ```java
  final int minimumPodId = 7;
  final Result<? extends Record> result = model.solve("POD", (table) -> {
      if (table.getName().equalsIgnoreCase("pod")) {
          // Should only pull in the 2nd record
          return conn.selectFrom(table).where(field("pod_id").gt(minimumPodId)).fetch();
      }
      return conn.selectFrom(table).fetch();
  });
  ``` 


### Finding out which constraints were unsatisfiable

If `model.solve()` fails, a `SolverException` is thrown. If the model was proven to be unsatisfiable,
the or-tools solver will also compute the set of offending constraints. This set can be accessed
via the `SolverException.core()` method. For now, `core()` returns only the string names of constraint
views that were unsatisfiable. We are currently working on returning fine-grained information about
which table-rows contributed to the unsatisfiability. 

<!-- embedme ../dcm/src/test/java/com/vmware/dcm/backend/ortools/CoreTest.java#L131-L156 -->
```java
final DSLContext conn = DSL.using("jdbc:h2:mem:");
conn.execute("create table t1(id integer, controllable__var integer)");
conn.execute("insert into t1 values (1, null)");
conn.execute("insert into t1 values (2, null)");
conn.execute("insert into t1 values (3, null)");

// Unsatisfiable
final String allDifferent = "create constraint constraint_all_different as " +
        "select * from t1 check all_different(controllable__var) = true";

// Unsatisfiable
final String domain1 = "create constraint constraint_domain_1 as " +
        "select * from t1 check controllable__var >= 1 and controllable__var <= 2";

// Satisfiable
final String domain2 = "create constraint constraint_domain_2 as " +
        "select * from t1 check id != 1 or controllable__var = 1";

final Model model = Model.build(conn, List.of(allDifferent, domain1, domain2));
try {
    model.solve("T1");
    fail();
} catch (final SolverException exception) {
    System.out.println(exception.core());
    assertTrue(exception.core().containsAll(List.of("CONSTRAINT_ALL_DIFFERENT", "CONSTRAINT_DOMAIN_1")));
    assertFalse(exception.core().contains("CONSTRAINT_DOMAIN_2"));
```

## Writing constraints

### Variable columns

Every DCM model computes values for one or more *variable columns*. A variable column is a column whose name
is prefixed with the keyword `controllable__`. A variable column can be of integer, bigint or
varchar types. DCM guarantees that a returned solution will assign values to variable columns such that
all hard constraints are satisfied, while maximizing the sum of supplied objective functions.

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
<!-- embedme ../k8s-scheduler/src/main/java/com/vmware/dcm/DBViews.java#L88-L90 -->
```sql
final String query = "SELECT pod_info.*, node_name AS controllable__node_name " +
                     "FROM pod_info " +
                     "WHERE status = 'Pending' AND node_name IS NULL AND schedulerName = 'dcm-scheduler'";
```

### Hard constraints

A hard constraint is structured as a `CREATE CONSTRAINT` statement that specifies a relation followed by a `CHECK` 
clause.  The `CHECK` clause  specifies a predicate that must hold true for every record produced by the relation. 
For example,

```sql
create constraint simple as
select * from virtual_machine
check controllable__physical_machine = 'pm3';
```

### Objective functions

An objective function is structured as a `CREATE CONSTRAINT` statement that specifies a relation followed by a 
`MAXIMIZE` clause.  The `MAXIMIZE` clause computes a single column of integers (booleans are automatically
cast into integers),  whose value DCM will try to maximize. For example,

```sql
create constraint objective_load_cpu as 
select * from spare_cpu
maximize min(cpu_spare);
```

or

```sql
create constraint objective_load_cpu as 
select * from spare_cpu
maximize cpu_spare;
```

DCM will try to maximize the sum of all objective functions. An objective function view that computes a 
column expression (like the second example above) is treated as one objective function per cell in the column.

### Supported column types for inputs

#### integer
#### bigint
#### varchar
#### array


### Expression types

A `NumExpr` may be an `integer` column or literal, a `bigint` column or literal,
or an arithmetic expression. A `BoolExpr` will be automatically cast into an integer value
if needed (`true = 1` and `false = 0`), and can therefore be used in places expecting a `NumExpr`.
`Expr` is any expression.  

An expression can be of variable type `Var` (i.e, derived from a variable column) or `Const` (constant type). 
Unless specified otherwise, an argument can be either `Var` or `Const`.

### Boolean operators

Binary operators with arguments of type `Expr` require both arguments to have the same type.

Name | Operator | Arguments | Example
--- | --- | --- | ---
Boolean AND | `AND`| `BoolExpr AND BoolExpr`| `CHECK (column_a = 10 AND column_b < 100)` 
Boolean OR | `OR` | `BoolExpr OR BoolExpr`| `CHECK (column_a = 10 OR column_b < 100)`
Equals | `=` | `Expr = Expr`| `CHECK (column_a = 10)`
Not equals | `!=` | `Expr = Expr`| `CHECK (column_a != 10)`
Greater than | `>` | `NumExpr > NumExpr`| `CHECK (column_a > 10)`
Greater than or equal to | `>=` | `NumExpr >= NumExpr`| `CHECK (column_a >= 10)`
Less than | `<` | `NumExpr < NumExpr`| `CHECK (column_a < 10)`
Less than or equal to | `<=` | `NumExpr <= NumExpr`| `CHECK (column_a <= 10)`
In | `IN` | `Expr IN (SELECT Expr FROM....)`| `CHECK (column_a IN (SELECT column_b FROM mytable))`
Exists | `EXISTS` | `EXISTS (SELECT Expr FROM...)`| `CHECK EXISTS (SELECT column_a = 10 FROM...)`
Array contains | `CONTAINS` | `CONTAINS (ARRAY expr, ARRAY column)`| `CHECK CONTAINS (column_arr, controllable__a)`

### Arithmetic operators

Name | Operator | Arguments | Example
--- | --- | --- | ---
Plus | `+`| `NumExpr + NumExpr`| `CHECK (column_a + column_b = 10)`
Minus | `-`| `NumExpr - NumExpr`| `CHECK (column_a - column_b = 10)`
Negation | `-`| `- NumExpr`| `CHECK (-column_a = 10)`
Multiplication | `*`| `NumExpr * NumExpr`| `CHECK (column_a * column_b = 10)`
Integer division | `/`| `NumExpr / NumExpr`| `CHECK (column_a / column_b = 10)`
Modulus | `%`| `NumExpr % NumExpr`| `CHECK (column_a % column_b = 10)`

### Supported aggregates

Some aggregates are *top-level constraints only*. This means that they can only be used on their own in a `CHECK` clause
as shown below. Their results cannot be used in a larger expression (other than to check for equality to `true`).

Operator | Arguments | Top-level only? | Examples | Description 
--- | --- | --- | --- | ---
`sum`| `sum(NumExpr)`| No | `SELECT sum(column_a) FROM ...`, `CHECK sum(column_a) = 10` | 
`count`| `count(NumExpr)`| No |  `SELECT count(column_a) FROM ...`, `CHECK count(column_a) = 10` |
`min`| `min(NumExpr)`| No | `SELECT min(column_a) FROM ...`, `CHECK min(column_a) = 10` |
`max`| `max(NumExpr)`| No | `SELECT max(column_a) FROM ...`, `CHECK max(column_a) = 10` |
`any`| `any(BoolExpr)` | Yes | `CHECK any(column_a)` | Enforce at least one entry in the column to be true 
`all`| `all(BoolExpr)` | Yes | `CHECK all(column_a)` | Enforce all entries in the column to be true
`all_different`| `all_different(NumExpr)` | Yes | `CHECK all_different(column_a)` | Enforce all values in the column to be mutually different.
`all_equal`| `all_equal(Expr)` | Yes | `CHECK all_equal(column_a)` | Enforce all values in the column to be equal.
`increasing`| `increasing(NumExpr)`| Yes | `CHECK increasing(column_a)` | Enforce ascending order for all values in this column
`capacity_constraint`| `capacity_constraint(Var NumExpr, Const NumExpr, Const NumExpr, Const NumExpr)`| Yes | `CHECK capacity_constraint(controllable_a, domain, demand, capacity)` | Assign values to `controllable_a` from `domain`, such that the total `demand` does not exceed the `capacity`. The length of the `controllable_a` argument should match that of the `demand` argument. The length of the `domain` argument should match that of the `capacity` argument.
