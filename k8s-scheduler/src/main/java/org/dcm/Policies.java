/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class Policies {
    private static final Map<PolicyName, Policy> ALL_POLICIES = new EnumMap<>(PolicyName.class);

    enum PolicyName {
        NODE_PREDICATES
    }

    static {
        ALL_POLICIES.put(PolicyName.NODE_PREDICATES, nodePredicates());
    }

    private static Policy nodePredicates() {
        final String constraint = "create view constraint_node_predicates as " +
                "select * " +
                "   from pods_to_assign " +
                "join node_info " +
                "on pods_to_assign.controllable__node_name = node_info.name " +
                "where node_info.unschedulable = false and " +
                "      node_info.memory_pressure = false and " +
                "      node_info.disk_pressure = false and " +
                "      node_info.pid_pressure = false and " +
                "      node_info.network_unavailable = false and " +
                "      node_info.ready = true ";
        return new Policy(PolicyName.NODE_PREDICATES, constraint);
    }

    static List<String> getDefaultPolicies() {
        return ALL_POLICIES.values().stream().map(e -> e.views)
                                            .flatMap(Collection::stream)
                                            .collect(Collectors.toList());
    }

    private static class Policy {
        private final PolicyName name;
        private final List<String> views;

        private Policy(final PolicyName name, final List<String> views) {
            this.name = name;
            this.views = views;
        }

        private Policy(final PolicyName name, final String views) {
            this.name = name;
            this.views = Collections.singletonList(views);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
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