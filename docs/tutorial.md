## Load balancer tutorial

In this tutorial, we will build 
a simple load balancer that distributes VMs across physical machines using DCM.
The code for this tutorial can be found in the [examples](../examples/) folder.
Start reading from the 
[LoadBalance](../examples/src/main/java/com/vmware/dcm/examples/LoadBalance.java) class.

1. The first step is to set up a database connection and instantiate a model using
   `Model.build()`. The `setup()` helper method returns a [JOOQ](https://github.com/jooq/) connection to an 
   initialized, in-memory database for our example. We will describe the kinds of constraints we can pass to the 
   model starting from step 5:

   <!-- embedme ../examples/src/main/java/com/vmware/dcm/examples/LoadBalance.java#L41-L44 -->
   ```java
   LoadBalance(final List<String> constraints) {
       conn = setup();
       model = Model.build(conn, constraints);
   }
   ```

   The database returned by the `setup()` method is initialized with the following schema, representing a 
   table each for physical and virtual machines:

    <!-- embedme ../examples/src/main/resources/schema.sql#L5-L18 -->
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
   Here, the `virtual_machine` table has a special column, `controllable__physical_machine`. 
   The `controllable__` prefix signals to DCM that it has to
   assign values to this column to satisfy all the supplied constraints. In doing so, we expect DCM to help us
   find a mapping of virtual machines to physical machines. 

2. We populate the tables with some data using a few helper methods (`LoadBalance.addVirtualMachine()` and
   `LoadBalance.addPhysicalMachine()`).

3. To actually run the load balancer, we simply invoke `LoadBalance.run()`, which uses two key methods from the DCM
   Model API -- `model.updateData()` and `model.solve()`:

   <!-- embedme ../examples/src/main/java/com/vmware/dcm/examples/LoadBalance.java#L78-L85 -->
   ```java
   Result<? extends Record> run() {
       // Pull the latest state from the DB
       model.updateData();
   
       // Run the solver and return the virtual machines table with solver-identified values for the
       // controllable__physical_machines column
       return model.solve(VIRTUAL_MACHINES_TABLE);
   }
   ```

4. Let's now drive our simple example with some tests. Have a look at the 
   [LoadBalanceTest.java](../examples/src/test/java/com/vmware/dcm/examples/LoadBalanceTest.java) class.
   All tests populate the database with the following initial state using the `LoadBalanceTest.addInventory()` 
   method:
  
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
 
5. Let's now introduce some constraints. We'll start with a very simple hard constraint: 
   assign all VMs to physical machine `pm3`:

   <!-- embedme ../examples/src/test/java/com/vmware/dcm/examples/LoadBalanceTest.java#L25-L41 -->
   ```java
   private static final int NUM_PHYSICAL_MACHINES = 5;
   private static final int NUM_VIRTUAL_MACHINES = 10;
   
   /*
    * A simple constraint that forces all assignments to go the same node
    */
   @Test
   public void testSimpleConstraint() {
       final String allVmsGoToPm3 = "create view constraint_simple as " +
                                    "select * from virtual_machine " +
                                    "check controllable__physical_machine = 'pm3'";
       final LoadBalance lb = new LoadBalance(Collections.singletonList(allVmsGoToPm3));
       addInventory(lb);
       final Result<? extends Record> results = lb.run();
       System.out.println(results);
       results.forEach(e -> assertEquals("pm3", e.get("CONTROLLABLE__PHYSICAL_MACHINE")));
   }
   ```
   To specify a hard constraint, we create a view with a `check` clause. The `check` clause
   specifies a predicate that should hold true for all records returned by the relation being selected (in this case,
   the table `virtual_machine`). This particular constraint requires all VMs to be assigned to `pm3`, which yields
   the following result when `model.solve` is invoked:

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

6. Now let's get serious and add a capacity constraint. We want to ensure that the
   sum of demands from all VMs assigned to a physical machine does not exceed the capacity of that machine. 
   To specify that, we write the following constraint:

   <!-- embedme ../examples/src/test/java/com/vmware/dcm/examples/LoadBalanceTest.java#L43-L67 -->
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
               "check sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
               "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";
   
       final LoadBalance lb = new LoadBalance(Collections.singletonList(capacityConstraint));
       addInventory(lb);
       final Result<? extends Record> results = lb.run();
       System.out.println(results);
       final Set<String> setOfPhysicalMachines = results.stream()
                                                    .map(e -> e.get("CONTROLLABLE__PHYSICAL_MACHINE", String.class))
                                                    .collect(Collectors.toSet());
       assertTrue(setOfPhysicalMachines.size() >= 2);
   }
   ```
   Here, the `check` clause specifies the capacity constraint. On my machine, this assigns all VMs evenly
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
   
7. Note that the constraints we have seen so far are hard constraints. Let's now add a soft constraint, a load balancing
   objective:
   
   <!-- embedme ../examples/src/test/java/com/vmware/dcm/examples/LoadBalanceTest.java#L69-L104 -->
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
               "check sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
               "      sum(virtual_machine.memory) <= physical_machine.memory_capacity";
   
       final String spareCpu = "create view spare_cpu as " +
               "select physical_machine.cpu_capacity - sum(virtual_machine.cpu) as cpu_spare " +
               "from virtual_machine " +
               "join physical_machine " +
               "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
               "group by physical_machine.name, physical_machine.cpu_capacity";
   
       // Queries presented as objectives, will have their values maximized.
       final String distributeLoadCpu = "create view objective_load_cpu as " +
                                        "select * from spare_cpu " +
                                        "maximize min(cpu_spare)";
   
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
   view (`spare_cpu`) that computes the spare CPU capacity on each physical machine. We then state an objective function, 
   which is specified as an SQL query that returns a single scalar value or a column, followed by the `maximize`
   annotation.  DCM will try to find assignments that maximize the values returned by that query. 
   In this case, we maximize the minimum spare CPU capacity, which has the intended load
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

8. Constraints can also refer to views computed in the database. This allows you to push a significant
   amount of complexity to the database and leverage the database's strengths (e.g., indexes, joins, aggregates,
   a rich suite of functions, and even user-defined functions). 
   
   For example, let's compute a simple view that pulls a subset of VMs from the database according to some labels:

   <!-- embedme ../examples/src/main/resources/schema.sql#L20-L22 -->
   ```sql
   -- Constraints can refer to views computed in the database as well
   create view vm_subset as
   select * from virtual_machine where name ='vm1' or name = 'vm2';
   ```

   Next, let's write a policy that directs that subset of VMs to the same physical machine, `pm3`:

   <!-- embedme ../examples/src/test/java/com/vmware/dcm/examples/LoadBalanceTest.java#L106-L120 -->
   ```java
   /*
    * An example where we also refer to views computed in the database
    */
   @Test
   public void testDatabaseViews() {
       final String someVmsGoToPm3 = "create view constraint_simple as " +
               "select * from virtual_machine " +
               "check name not in (select name from vm_subset) or controllable__physical_machine = 'pm3'";
   
       final LoadBalance lb = new LoadBalance(List.of(someVmsGoToPm3));
       addInventory(lb);
       final Result<? extends Record> result = lb.run();
       result.stream().filter(e -> e.get("NAME").equals("vm1") || e.get("NAME").equals("vm2"))
             .forEach(e -> assertEquals("pm3", e.get("CONTROLLABLE__PHYSICAL_MACHINE")));
   }
   ```
   
9. Constraints may not always be satisfiable. When a model is unsatisfiable, `model.solve()` throws a `SolverException`,
   which has a `core()` method that returns the list of failed constraints:

   <!-- embedme ../examples/src/test/java/com/vmware/dcm/examples/LoadBalanceTest.java#L122-L157 -->
   ```java
   /*
    * We now introduce two mutually unsatisfiable constraints to showcase the UNSAT core API
    */
   @Test
   public void testUnsat() {
       // Satisfiable
       final String someVmsAvoidPm3 = "create view constraint_some_avoid_pm3 as " +
               "select * from virtual_machine " +
               "check name not in (select name from vm_subset) or controllable__physical_machine != 'pm3'";
   
       // The next two constraints are mutually unsatisfiable. The first constraint forces too many VMs
       // to go the same physical machine, but that violates the capacity constraint
       final String restGoToPm3 = "create view constraint_rest_to_pm3 as " +
               "select * from virtual_machine " +
               "check name in (select name from vm_subset) or controllable__physical_machine = 'pm3'";
   
       final String capacityConstraint =
               "create view constraint_capacity as " +
               "select * from virtual_machine " +
               "join physical_machine " +
               "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
               "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity " +
               "check sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
               "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";
   
       final LoadBalance lb = new LoadBalance(List.of(someVmsAvoidPm3, restGoToPm3, capacityConstraint));
       addInventory(lb);
       try {
           lb.run();
           fail();
       } catch (final SolverException e) {
           System.out.println(e.core());
           assertTrue(e.core().containsAll(List.of("constraint_rest_to_pm3", "constraint_capacity")));
           assertFalse(e.core().contains("constraint_some_avoid_pm3"));
       }
   }
   ```