/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class Policies {
    private static final List<Policy> ALL_POLICIES = new ArrayList<>();

    static {
        ALL_POLICIES.add(nodePredicates());
    }

    /**
     * Ensures that the pods_to_assign.constraint_controllable__node_name column is assigned to nodes
     * that satisfy some predicates corresponding to availability and resource utilization
     */
    static Policy nodePredicates() {
        final String constraint = "create view constraint_controllable_node_name_domain as " +
                                  "select * from pods_to_assign " +
                                  "where controllable__node_name in " +
                                        "(select name from node_info" +
                                        "  where node_info.unschedulable = false and " +
                                        "        node_info.memory_pressure = false and " +
                                        "        node_info.out_of_disk = false and " +
                                        "        node_info.disk_pressure = false and " +
                                        "        node_info.pid_pressure = false and " +
                                        "        node_info.network_unavailable = false and " +
                                        "        node_info.ready = true)";
        return new Policy("NodePredicates", constraint);
    }

    static List<String> getDefaultPolicies() {
        return from(ALL_POLICIES);
    }

    static List<String> from(final Policy policy) {
        return from(Collections.singletonList(policy));
    }

    static List<String> from(final List<Policy> policies) {
        return policies.stream().map(e -> e.views)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private static class Policy {
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