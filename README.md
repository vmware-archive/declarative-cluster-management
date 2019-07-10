A tool that enables programmers to specify cluster management policies in a
high-level declarative language, and compute policy-compliant configurations
automatically and efficiently. Weave allows constraints and policies, the
essence of a cluster manager, to be easily added, removed and modified
over time, using a language familiar to developers (SQL).

### Pre-requisites

We currently support only the MiniZinc solver backend. You can download it from: minizinc.org.

Make sure you are able to invoke the `minizinc` binary from your commandline.


### Building

 $: mvn package


### References

* Lalith Suresh, João Loff, Nina Narodytska, Leonid Ryzhyk, Mooly Sagiv, and Brian Oki. 2019.
  Synthesizing Cluster Management Code for Distributed Systems.
  In Proceedings of the Workshop on Hot Topics in Operating Systems (HotOS '19).
  ACM, New York, NY, USA, 45-50. DOI: https://doi.org/10.1145/3317550.3321444

