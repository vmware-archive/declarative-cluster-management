## Changelog


# Release 0.8.0

* Public API changes:
  * Objective functios no longer need to be scalar expressions, but
    can be any column expression. Each value in the column will be
    treated as an objective function. (#81, #82)
* Issues fixed: #41, 81, #82, #85, #86


# Release 0.7.1

* API improvements
  * A fetcher API for supplying input data to tables: https://github.com/vmware/declarative-cluster-management/commit/bfdbb7951aa4944e770fef5e5a16318ad12778e8
  * Reworks exceptions thrown by the compiler and solver (#73, #74).
* Bug fixes: 
  * dcm: `all_equal()` now correctly works against variable columns
  * dcm: fixes capacity constraint bugs related to empty domains, working with bigint/long columns, overflows, and divide by zeroes (#73, #75)
  * k8s-scheduler: use pod_info.uid to uniquely identify pods because pod_info.name is not unique across namespaces (#72)

