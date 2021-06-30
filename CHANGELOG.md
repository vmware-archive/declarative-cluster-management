# Changelog

## Ongoing: Release 0.11.0
* Public API changes:
  * #114: dcm: simplify Model and ISolverBackend APIs
  * #96: Use consistent syntax for hard and soft constraints

* Issues fixed:
  * #95: k8s-scheduler,benchmarks: eliminate stray running threads
  * #96: dcm: Use consistent syntax for hard and soft constraints
  * #101,#102: ortools-backend: improve intermediate view type inferrence
  * #107: ScaleNodeBenchmark to measure solver's latency
  * #109: dcm: make sure string literals in generated code preserve the casing from the DB
  * #110,#113: Don't search for an UNSAT core unless solver status is INFEAS…IBLE
  * Overhaul of internal APIs from Model -> Backend
  
## June 2, 2021: Release 0.10.0
* Public API changes:  
  * Upgrade to Google OrTools 9.0.9048  
  * #93: UNSAT core interface  

* Issues fixed:
  * #93: Add UNSAT core interface   
  * #94: re-organize compiler code as a pipeline of passes against a `Program<T>` representation  

## Apr 27, 2021: Release 0.9.0
* Public API changes:
   * With #91, no longer requires Jcenter repository
   * Upgrade to Google OrTools 8.2.9025

* Issues fixed:
  * #91: Migrate to official ortools Maven dependency  
  * #89: ortools: using sharper types in backend code generator  
  * #45: Reduce presolve times enhancement 
  * Several performance improvements 

## Mar 24, 2021: Release 0.8.0

* Public API changes:
  * Objective functions no longer need to be scalar expressions, but
    can be any column expression. Each value in the column will be
    treated as an objective function. (#81, #82)
* Issues fixed: #41, 81, #82, #85, #86


## Feb 8, 2021: Release 0.7.1

* API improvements
  * A fetcher API for supplying input data to tables: https://github.com/vmware/declarative-cluster-management/commit/bfdbb7951aa4944e770fef5e5a16318ad12778e8
  * Reworks exceptions thrown by the compiler and solver (#73, #74).
* Bug fixes: 
  * dcm: `all_equal()` now correctly works against variable columns
  * dcm: fixes capacity constraint bugs related to empty domains, working with bigint/long columns, overflows, and divide by zeroes (#73, #75)
  * k8s-scheduler: use pod_info.uid to uniquely identify pods because pod_info.name is not unique across namespaces (#72)


## Nov 10, 2020: Release 0.6.0

* or-tools backend and public API changes:
  * Views with check clauses can now also have a where clause


## Oct 30, 2020: Release 0.5.0

* ortools-backend:
  * Add all_different() constraint


## Oct 30, 2020: Release 0.4.0

* API changes:
  * model.solve() no longer returns tables without variables
  * removed solveModelAndReflectTableChanges() testing API
  * Improvements to the or-tools backend (evaluating constant sub-queries only once and better support for nested queries).


## Sep 23, 2020: Release 0.3.0
## Sep 21, 2020: Release 0.2.0

