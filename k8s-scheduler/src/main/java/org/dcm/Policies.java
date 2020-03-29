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
    private static final List<Policy> ALL_POLICIES = new ArrayList<>();

    static {
        ALL_POLICIES.add(nodePredicates());
        ALL_POLICIES.add(nodeSelectorPredicate());
        ALL_POLICIES.add(podAffinityPredicate());
        ALL_POLICIES.add(podAntiAffinityPredicate());
        ALL_POLICIES.add(capacityConstraint(true, true));
        ALL_POLICIES.add(taintsAndTolerations());
        ALL_POLICIES.add(symmetryBreaking());
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy some predicates corresponding to availability and resource utilization
     */
    static Policy nodePredicates() {
        final String constraint = "create view constraint_controllable_node_name_domain as " +
                                  "select * from pods_to_assign " +
                                  "where controllable__node_name in " +
                                        "(select name from spare_capacity_per_node)";
        return new Policy("NodePredicates", constraint);
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
                                  "          where pods_to_assign.pod_name = pod_node_selector_matches.pod_name)";
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
            "          and inter_pod_affinity_matches.node_name is not null)"; // running pods
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
            "          and inter_pod_affinity_matches.node_name is not null)"; // running pods
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
        final String hardConstraint = "create view constraint_pods_slack_per_node as " +
            "select * " +
            "from spare_capacity_per_node " +
            "join pods_to_assign " +
            "     on pods_to_assign.controllable__node_name = spare_capacity_per_node.name " +
            "having capacity_constraint(pods_to_assign.controllable__node_name, spare_capacity_per_node.name, " +
            "                           pods_to_assign.cpu_request, spare_capacity_per_node.cpu_remaining) = true" +
            " and capacity_constraint(pods_to_assign.controllable__node_name, spare_capacity_per_node.name, " +
            "                         pods_to_assign.memory_request, spare_capacity_per_node.memory_remaining) = true" +
            " and capacity_constraint(pods_to_assign.controllable__node_name, spare_capacity_per_node.name, " +
            "                         pods_to_assign.pods_request, spare_capacity_per_node.pods_remaining) = true";
        views.add(hardConstraint);
        // TODO: Add soft constraint only version as well
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


    static List<String> getAllPolicies() {
        return from(ALL_POLICIES);
    }

    static List<String> getDefaultPolicies() {
        return from(ALL_POLICIES);
    }

    static List<String> getDefaultPoliciesWithPodsToAssignReplaced(final String podsToAssignReplacement) {
        return ALL_POLICIES.stream()
                    .map(policy -> policy.views)
                    .flatMap(Collection::stream)
                    .map(view -> view.replaceAll("pods_to_assign", podsToAssignReplacement))
                    .collect(Collectors.toList());
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