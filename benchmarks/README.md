# Running benchmarks

This sub-project hosts a suite of micro-benchmarks that we use to inform
our design choices in DCM.

To run these benchmarks, execute the following commands from the project
root folder:

```
  $: mvn package
  $: java -jar benchmarks/target/benchmark.jar
```