[![Build Status](https://travis-ci.com/vmware/declarative-cluster-management.svg?branch=master)](https://travis-ci.com/vmware/declarative-cluster-management)
[![codecov](https://codecov.io/gh/vmware/declarative-cluster-management/branch/master/graph/badge.svg)](https://codecov.io/gh/vmware/declarative-cluster-management)

# Declarative Cluster Management

## Overview

A tool that enables programmers to specify cluster management policies in a
high-level declarative language, and compute policy-compliant configurations
automatically and efficiently. DCM allows constraints and policies, the
essence of a cluster manager, to be easily added, removed and modified
over time, using a language familiar to developers (SQL).

## Try it out

### Pre-requisites

We currently support only the MiniZinc solver backend. You can download it from: minizinc.org.

Make sure you are able to invoke the `minizinc` binary from your commandline.


### Building

 $: mvn package

### References

To learn more about DCM, we suggest going through the following research papers:

* [Automating Cluster Management with Weave](https://arxiv.org/pdf/1909.03130.pdf)<br>
  Lalith Suresh, Joao Loff, Faria Kalim, Nina Narodytska, Leonid Ryzhyk, Sahan Gamage, Brian Oki, Zeeshan Lokhandwala, Mukesh Hira, Mooly Sagiv. arXiv preprint arXiv:1909.03130 (2019).

* [Synthesizing Cluster Management Code for Distributed Systems](https://dl.acm.org/citation.cfm?id=3321444)<br>
  Lalith Suresh, João Loff, Nina Narodytska, Leonid Ryzhyk, Mooly Sagiv, and Brian Oki. In Proceedings of the Workshop on Hot Topics in Operating Systems (HotOS 2019).
  ACM, New York, NY, USA, 45-50. DOI: https://doi.org/10.1145/3317550.3321444

