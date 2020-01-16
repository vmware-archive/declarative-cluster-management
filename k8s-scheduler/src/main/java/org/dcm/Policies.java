/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Policies {
    private static final List<Policy> INITIAL_PLACEMENT_POLICIES = new ArrayList<>();
    private static final List<Policy> DESCHEDULING_POLICIES = new ArrayList<>();

    static {
        INITIAL_PLACEMENT_POLICIES.add(nodePredicates());
        INITIAL_PLACEMENT_POLICIES.add(nodeSelectorPredicate());
        INITIAL_PLACEMENT_POLICIES.add(podAffinityPredicate());
        INITIAL_PLACEMENT_POLICIES.add(podAntiAffinityPredicate());
        INITIAL_PLACEMENT_POLICIES.add(capacityConstraint(true, true));
        INITIAL_PLACEMENT_POLICIES.add(taintsAndTolerations());
        INITIAL_PLACEMENT_POLICIES.add(symmetryBreaking());

        DESCHEDULING_POLICIES.addAll(INITIAL_PLACEMENT_POLICIES);
        DESCHEDULING_POLICIES.add(deschedulingConstraints());
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy some predicates corresponding to availability and resource utilization
     */
    static Policy nodePredicates() {
        final String validNodes = "create view constraint_controllable_node_name_domain as " +
                                  "select * from pods_to_assign " +
                                  "where controllable__node_name in (select name from allowed_nodes) " +
                                  " or controllable__node_name = 'NULL_NODE'";
        final String softConstraintToMinimizeUnassignedPods =
                        "create view objective_minimize_nulls as " +
                        "select -sum(controllable__node_name = 'NULL_NODE') * 100 from pods_to_assign";
        return new Policy("NodePredicates", List.of(validNodes, softConstraintToMinimizeUnassignedPods));
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy node affinity requirements. This policy covers the basic node selector as well as
     * the nodeAffinity policies in k8s.
     */
    static Policy nodeSelectorPredicate() {
        final String constraint = "create view constraint_node_selector as " +
                                  "select * " +
                                  "from pods_to_assign " +
                                  "where pods_to_assign.has_node_selector_labels = false or " +
                                  "      pods_to_assign.controllable__node_name in " +
                                  "         (select node_name " +
                                  "          from pod_node_selector_matches " +
                                  "          where pods_to_assign.pod_name = pod_node_selector_matches.pod_name)" +

                                  // Else infeasible
                                  "   or controllable__node_name = 'NULL_NODE'";
        return new Policy("NodeSelectorPredicate", constraint);
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy pod affinity requirements.
     */
    static Policy podAffinityPredicate() {
        final String constraint = "create view constraint_pod_affinity as " +
                                  "select * " +
                                  "from pods_to_assign " +
                                  "where pods_to_assign.has_pod_affinity_requirements = false or " +

            // Affinity to pending pods: for pods_to_assign.pod_name, find the pending pods that are affine to it
            // from inter_pod_affinity_matches. We get this latter set of pending pods by joining
            // inter_pod_affinity_matches with pods_to_assign (inner).
            "      (pods_to_assign.controllable__node_name in " +
            "         (select b.controllable__node_name" +
            "          from pods_to_assign as b" +
            "          join inter_pod_affinity_matches" +
            "           on inter_pod_affinity_matches.pod_name = pods_to_assign.pod_name" +
            "           and inter_pod_affinity_matches.matches = b.pod_name" +
                        // If the pod is affine only to itself and no others, it can be placed anywhere
            "           and (num_matches = 1 or inter_pod_affinity_matches.matches != pods_to_assign.pod_name)" +
            "           and inter_pod_affinity_matches.node_name is null))" + // pending pods

            // Affinity to running pods...
            "   or pods_to_assign.controllable__node_name in " +
            "         (select inter_pod_affinity_matches.node_name " +
            "          from inter_pod_affinity_matches " +
            "          where pods_to_assign.pod_name = inter_pod_affinity_matches.pod_name " +
            "          and inter_pod_affinity_matches.node_name is not null)" + // running pods

            // Else infeasible
            "   or controllable__node_name = 'NULL_NODE'";
        return new Policy("InterPodAffinity", constraint);
    }


    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy pod anti0affinity requirements.
     */
    static Policy podAntiAffinityPredicate() {
        final String constraint = "create view constraint_pod_anti_affinity as " +
            "select * " +
            "from pods_to_assign " +
            "where pods_to_assign.has_pod_anti_affinity_requirements = false or " +

            // Affinity to pending pods: for pods_to_assign.pod_name, find the pending pods that are affine to it
            // from inter_pod_affinity_matches. We get this latter set of pending pods by joining
            // inter_pod_affinity_matches with pods_to_assign (inner).
            "      (pods_to_assign.controllable__node_name not in " +
            "         (select b.controllable__node_name" +
            "          from pods_to_assign as b" +
            "          join inter_pod_anti_affinity_matches" +
            "           on inter_pod_anti_affinity_matches.pod_name = pods_to_assign.pod_name" +
            "           and inter_pod_anti_affinity_matches.matches != pods_to_assign.pod_name" +
                        // next clause assumes that a pod cannot be anti-affine to itself
            "           and inter_pod_anti_affinity_matches.matches = b.pod_name" +
            "           and inter_pod_anti_affinity_matches.node_name is null))" + // pending pods

            // Affinity to running pods...
            "   or pods_to_assign.controllable__node_name in " +
            "         (select inter_pod_affinity_matches.node_name " +
            "          from inter_pod_affinity_matches " +
            "          where pods_to_assign.pod_name = inter_pod_affinity_matches.pod_name " +
            "          and inter_pod_affinity_matches.node_name is not null)" + // running pods

            // Else infeasible
            "   or controllable__node_name = 'NULL_NODE'";
        return new Policy("InterPodAntiAffinity", constraint);
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
        final String intermediateView = "create view pods_slack_per_node as " +
            "select sum((pods_to_assign.cpu_request * 100) / spare_capacity_per_node.cpu_remaining) " +
                "      as cpu_load," +
            "  sum((pods_to_assign.memory_request * 100) / spare_capacity_per_node.memory_remaining) " +
                "      as memory_load " +
            "from spare_capacity_per_node " +
            "join pods_to_assign " +
            "     on pods_to_assign.controllable__node_name = spare_capacity_per_node.name " +
            "group by spare_capacity_per_node.name, spare_capacity_per_node.cpu_remaining, " +
            "         spare_capacity_per_node.memory_remaining, spare_capacity_per_node.pods_remaining";
        final String capacityHardConstraint =  "create view constraint_capacity as " +
                                               "select * from pods_slack_per_node " +
                                               "where cpu_load <= 100 " +
                                               "  and memory_load <= 100";
        final String capacityCpuMemSoftConstraint = "create view objective_least_requested_cpu_mem as " +
                                                    "select -max(cpu_load + memory_load) " +
                                                    "from pods_slack_per_node";
        views.add(intermediateView);
        if (withHardConstraint) {
            views.add(capacityHardConstraint);
        }
        if (withSoftConstraint) {
            views.add(capacityCpuMemSoftConstraint);
        }
        return new Policy("CapacityConstraint", views);
    }

    /**
     * All pods belonging to the same owner are symmetric with respect to one another.
     */
    static Policy symmetryBreaking() {
        final String constraint = "create view constraint_symmetry_breaking as " +
                "select * " +
                "from pods_to_assign " +
                "group by equivalence_class " +
                "having increasing(pods_to_assign.controllable__node_name) = true";
        return new Policy("SymmetryBreaking", constraint);
    }

    /**
     * Node taints and tolerations
     */
    static Policy taintsAndTolerations() {
        final String constraint = "create view constraint_node_taints as " +
                "select * " +
                "from pods_to_assign " +
                "join nodes_that_have_tolerations" +
                "    on pods_to_assign.controllable__node_name = nodes_that_have_tolerations.node_name " +
                "where exists(select * from pods_that_tolerate_node_taints as A " +
                "              where A.pod_name = pods_to_assign.pod_name" +
                "                and A.node_name = pods_to_assign.controllable__node_name) = true";
        return new Policy("NodeTaintsPredicate", constraint);
    }

    /**
     * Descheduling policies
     */
    static Policy deschedulingConstraints() {
        final String allowPodEvictionsButNotReassignment =  "create view constraint_descheduling as " +
                                                            "select * " +
                                                            "from pods_to_assign " +
                                                            "where controllable__node_name = current_node_name " +
                                                            "   or controllable__node_name = 'NULL_NODE'";

        final String limitNumberOfEvictedTasks =  "create view constraint_eviction_limit as " +
                                                  "select * " +
                                                  "from pods_to_assign " +
                                                  "group by status " +
                                                  "having sum(controllable__node_name = 'NULL_NODE') < 5";
        return new Policy("DeschedulingConstraints", List.of(allowPodEvictionsButNotReassignment,
                                                                   limitNumberOfEvictedTasks));
    }

    static List<String> getInitialPlacementPolicies() {
        return from(INITIAL_PLACEMENT_POLICIES);
    }

    static List<String> getDeschedulingPolicies() {
        return from(DESCHEDULING_POLICIES);
    }

    static List<String> from(final Policy policy) {
        return from(Collections.singletonList(policy));
    }

    static List<String> from(final Policy... policies) {
        return Stream.of(policies).map(e -> e.views)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
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