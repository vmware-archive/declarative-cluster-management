# DCM Reference


DCM concepts

## Instantiating a Model

#### Model.build(DSLContext dslContext, List<String> constraints)

#### Model.build(DSLContext dslContext, ISolverBackend solverBackend, List<String> constraints)

## Model API

#### model.solve()

#### model.solve(String tableName)

#### model.updateData()

#### model.updateData(Function<Table<?>, Result<? extends Record>> fetcher)

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
