/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class Policies {
    private static final List<Policy> INITIAL_PLACEMENT_POLICIES = new ArrayList<>();
    private static final List<Policy> PREEMPTION_POLICIES = new ArrayList<>();

    static {
        INITIAL_PLACEMENT_POLICIES.add(disallowNullNodeSoft());
        INITIAL_PLACEMENT_POLICIES.add(nodePredicates());
        INITIAL_PLACEMENT_POLICIES.add(nodeSelectorPredicate());
        INITIAL_PLACEMENT_POLICIES.add(podAffinityPredicate());
        INITIAL_PLACEMENT_POLICIES.add(podAntiAffinityPredicate());
        INITIAL_PLACEMENT_POLICIES.add(capacityConstraint(true, true));
        INITIAL_PLACEMENT_POLICIES.add(taintsAndTolerations());
        INITIAL_PLACEMENT_POLICIES.add(symmetryBreaking());
        INITIAL_PLACEMENT_POLICIES.add(podFitsNodePorts());

        PREEMPTION_POLICIES.add(preemption());
        PREEMPTION_POLICIES.addAll(new ArrayList<>(INITIAL_PLACEMENT_POLICIES));
    }

    /**
     * Only allow pods with current assignments to be marked NULL_NODE
     */
    static Policy preemption() {
        final String constraint = "create constraint preemption_requirement as " +
                                  "select * from pods_to_assign " +
                                  "where node_name is not null " +
                                  "check controllable__node_name = 'NULL_NODE' " +
                                  "   or controllable__node_name = node_name";
        final String maximize = "create constraint preemption_objective as " +
                                "select * from pods_to_assign " +
                                "maximize priority * (controllable__node_name != 'NULL_NODE')";
        return new Policy("Preemption", List.of(constraint, maximize));
    }

    /**
     * Prefer to avoid NULL_NODE assignments
     */
    static Policy disallowNullNodeSoft() {
        final String constraint = "create constraint constraint_disallow_null_node as " +
                "select * from pods_to_assign " +
                "maximize controllable__node_name != 'NULL_NODE'";
        return new Policy("DisallowNullNode", constraint);
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy some predicates corresponding to availability and resource utilization
     */
    static Policy nodePredicates() {
        final String constraint = "create constraint constraint_controllable_node_name_domain as " +
                                  "select * from pods_to_assign " +
                                  "check controllable__node_name in " +
                                        "(select name from spare_capacity_per_node) " +
                                        "or controllable__node_name = 'NULL_NODE'";
        return new Policy("NodePredicates", constraint);
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy node affinity requirements. This policy covers the basic node selector as well as
     * the nodeAffinity policies in k8s.
     */
    static Policy nodeSelectorPredicate() {
        final String constraint = "create constraint constraint_node_selector as " +
                                  "select * " +
                                  "from pods_to_assign " +
                                  "check pods_to_assign.has_node_selector_labels = false or " +
                                  "      pods_to_assign.controllable__node_name in " +
                                  "         (select node_matches " +
                                  "          from pod_node_selector_matches " +
                                  "          where pods_to_assign.uid = pod_node_selector_matches.pod_uid) " +
                                  "or controllable__node_name = 'NULL_NODE'";
        return new Policy("NodeSelectorPredicate", constraint);
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy pod affinity requirements.
     */
    static Policy podAffinityPredicate() {
        final String constraint = "create constraint constraint_pod_affinity as " +
                                  "select * " +
                                  "from pods_to_assign " +
                                  "check pods_to_assign.has_pod_affinity_requirements = false or " +

            // Affinity to pending pods: for pods_to_assign.uid, find the pending pods that are affine to it
            // from inter_pod_affinity_matches_pending. We get this latter set of pending pods by joining
            // inter_pod_affinity_matches_pending with pods_to_assign (inner).
            "      (pods_to_assign.controllable__node_name in " +
            "         (select b.controllable__node_name" +
            "          from pods_to_assign as b" +
            "          join inter_pod_affinity_matches_pending" +
            "           on inter_pod_affinity_matches_pending.pod_uid = pods_to_assign.uid" +
            "           and contains(inter_pod_affinity_matches_pending.pod_matches, b.uid)" +
            "       ))" +
                // pending pods

                // Affinity to running pods...
            "   or pods_to_assign.controllable__node_name in " +
            "         (select inter_pod_affinity_matches_scheduled.node_matches " +
            "          from inter_pod_affinity_matches_scheduled " +
            "          where pods_to_assign.uid = inter_pod_affinity_matches_scheduled.pod_uid) " +  // running pods

            // Or infeasible
            "   or controllable__node_name = 'NULL_NODE'";
        return new Policy("InterPodAffinity", constraint);
    }


    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy pod anti-affinity requirements.
     */
    static Policy podAntiAffinityPredicate() {
        final String constraintPending = "create constraint constraint_pod_anti_affinity_pending as " +
            "select * " +
            "from pods_to_assign " +
            "check pods_to_assign.has_pod_anti_affinity_requirements = false or " +
            // Anti-affinity to pending pods: for pods_to_assign.uid, find the pending pods that are
            // anti-affine to it from inter_pod_anti_affinity_matches_pending. We get this latter set of
            // pending pods by joining inter_pod_anti_affinity_matches_pending with pods_to_assign (inner).
            "      (pods_to_assign.controllable__node_name not in " +
            "         (select b.controllable__node_name" +
            "          from pods_to_assign as b" +
            "          join inter_pod_anti_affinity_matches_pending" +
            "           on inter_pod_anti_affinity_matches_pending.pod_uid = pods_to_assign.uid" +
            "           and contains(inter_pod_anti_affinity_matches_pending.pod_matches, b.uid))) " +

            // Or infeasible
            "   or controllable__node_name = 'NULL_NODE'";

        final String constraintScheduled =
                "create constraint constraint_pod_anti_affinity_scheduled as " +
                "select * " +
                "from pods_to_assign " +
                "join inter_pod_anti_affinity_matches_scheduled on  " +
                "     pods_to_assign.uid = inter_pod_anti_affinity_matches_scheduled.pod_uid " +
                "check pods_to_assign.has_pod_anti_affinity_requirements = false or " +
                "      not(contains(inter_pod_anti_affinity_matches_scheduled.node_matches, " +
                "                   pods_to_assign.controllable__node_name)) " +
                // Or infeasible
                "   or controllable__node_name = 'NULL_NODE'";
        return new Policy("InterPodAntiAffinity", List.of(constraintPending, constraintScheduled));
    }

    /**
     * Avoid assigning pods to the same node if they have conflicting host port requirements
     */
    static Policy podFitsNodePorts() {
        final String constraintPending = """
                    CREATE CONSTRAINT constraint_pod_fits_node_ports_pending AS
                    SELECT *
                    FROM pods_to_assign
                    CHECK pods_to_assign.has_node_port_requirements = false OR
                          -- Anti-affinity to pending pods
                          (pods_to_assign.controllable__node_name NOT IN
                             (SELECT b.controllable__node_name
                              FROM pods_to_assign AS b
                              JOIN pods_with_port_requests_pending
                               ON  pods_with_port_requests_pending.pod_uid = pods_to_assign.uid
                                AND contains(pods_with_port_requests_pending.pod_matches, b.uid)))
                          -- Or infeasible
                          OR controllable__node_name = 'NULL_NODE'""";

        final String constraintScheduled = """
                   CREATE CONSTRAINT constraint_pods_with_port_requests_scheduled AS
                   SELECT *
                   FROM pods_to_assign
                   JOIN pods_with_port_requests_scheduled
                     ON pods_to_assign.uid = pods_with_port_requests_scheduled.pod_uid
                   CHECK pods_to_assign.has_node_port_requirements = false OR
                         NOT(CONTAINS(pods_with_port_requests_scheduled.node_matches,
                                      pods_to_assign.controllable__node_name))
                         -- Or infeasible
                         OR controllable__node_name = 'NULL_NODE'""";
        return new Policy("PodFitsNodePorts", List.of(constraintPending, constraintScheduled));
    }


    /**
     * Hard and soft capacity constraints over CPU, memory and the number of pods
     */
    static Policy capacityConstraint(final boolean withHardConstraint, final boolean withSoftConstraint) {
        Preconditions.checkArgument(withHardConstraint || withSoftConstraint);

        // We don't handle outer joins yet, so this policy currently assumes that there is at least one running
        // pod per node. If not, those nodes will lack a row in the spare_capacity_per_node view. This is fine for
        // Kubernetes, because there always some system pods running on each node.
        final List<String> views = new ArrayList<>();
        final String hardConstraint = """
                CREATE CONSTRAINT constraint_pods_slack_per_node AS
                SELECT *
                FROM pods_to_assign
                JOIN pod_resource_demands
                    ON pod_resource_demands.uid = pods_to_assign.uid
                JOIN spare_capacity_per_node
                     ON pod_resource_demands.resource = spare_capacity_per_node.resource
                GROUP BY spare_capacity_per_node.resource, pod_resource_demands.resource
                CHECK capacity_constraint(pods_to_assign.controllable__node_name, spare_capacity_per_node.name,
                                          pod_resource_demands.demand, spare_capacity_per_node.capacity) = true
                """;
        views.add(hardConstraint);
        // TODO: Add soft constraint only version as well
        return new Policy("CapacityConstraint", views);
    }

    /**
     * All pods belonging to the same owner are symmetric with respect to one another.
     */
    static Policy symmetryBreaking() {
        final String constraint = "create constraint constraint_symmetry_breaking as " +
                "select * " +
                "from pods_to_assign " +
                "group by equivalence_class " +
                "check increasing(pods_to_assign.controllable__node_name) = true";
        return new Policy("SymmetryBreaking", constraint);
    }

    /**
     * Node taints and tolerations
     */
    static Policy taintsAndTolerations() {
        final String constraint = "create constraint constraint_node_taints as " +
                "select * " +
                "from pods_to_assign " +
                "join nodes_that_have_tolerations" +
                "    on pods_to_assign.controllable__node_name = nodes_that_have_tolerations.node_name " +
                "check exists(select * from pods_that_tolerate_node_taints as A " +
                "              where A.pod_uid  = pods_to_assign.uid" +
                "                and A.node_name = pods_to_assign.controllable__node_name) = true";
        return new Policy("NodeTaintsPredicate", constraint);
    }


    static List<String> getInitialPlacementPolicies() {
        return from(INITIAL_PLACEMENT_POLICIES);
    }

    static List<String> getPreemptionPlacementPolicies() {
        return from(PREEMPTION_POLICIES);
    }

    static List<String> getInitialPlacementPolicies(final Policy... policies) {
        return from(List.of(policies));
    }

    static List<String> from(final List<Policy> policies) {
        return policies.stream().map(e -> e.views)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    static class Policy {
        private final String name;
        private final List<String> views;

        private Policy(final String name, final List<String> views) {
            this.name = name;
            this.views = views;
        }

        private Policy(final String name, final String views) {
            this.name = name;
            this.views = Collections.singletonList(views);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Policy)) {
                return false;
            }
            final Policy policy = (Policy) o;
            return name.equals(policy.name) && views.equals(policy.views);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, views);
        }

        @Override
        public String toString() {
            return "Policy{" +
                    "name='" + name + '\'' +
                    ", views=" + views +
                    '}';
        }
    }
}