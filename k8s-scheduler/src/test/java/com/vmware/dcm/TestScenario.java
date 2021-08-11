/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import org.jooq.Record;
import org.jooq.Result;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.vmware.dcm.SchedulerTest.newNode;
import static com.vmware.dcm.SchedulerTest.newPod;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * A simple DSL to set up scheduling scenarios for testing
 */
class TestScenario {
    private final DBConnectionPool dbConnectionPool = new DBConnectionPool();
    private final Scheduler.Builder schedulerBuilder = new Scheduler.Builder(dbConnectionPool);
    private final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
    private final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
    private final PodResourceEventHandler podResourceEventHandler = new PodResourceEventHandler(eventHandler::handle);
    private final Set<String> podGroups = new HashSet<>();
    private final Set<String> nodeGroups = new HashSet<>();
    private final List<Pod> pods = new ArrayList<>();
    private final List<Node> nodes = new ArrayList<>();
    @Nullable
    Scheduler scheduler;

    static TestScenario withInitialPlacementPolicies(final List<String> policies) {
        final var scenario = new TestScenario();
        scenario.schedulerBuilder.setInitialPlacementPolicies(policies);
        scenario.scheduler = scenario.schedulerBuilder.build();
        return scenario;
    }

    TestScenario withPodGroup(final String groupName, final int numPods, final Consumer<Pod>... modifiers) {
        if (podGroups.contains(groupName)) {
            throw new IllegalArgumentException("Pod group already exists: " + groupName);
        }
        podGroups.add(groupName);
        for (int i = 0; i < numPods; i++) {
            final Pod pod = newPod(groupName + "-" + UUID.randomUUID());
            for (final var modifier : modifiers) {
                modifier.accept(pod);
            }
            pods.add(pod);
            podResourceEventHandler.onAddSync(pod);
        }
        return this;
    }

    TestScenario withNodeGroup(final String groupName, final int numNodes, final Consumer<Node>... modifiers) {
        if (nodeGroups.contains(groupName)) {
            throw new IllegalArgumentException("Node group already exists: " + groupName);
        }
        nodeGroups.add(groupName);
        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode(groupName + "-" + UUID.randomUUID(),
                                      Collections.emptyMap(), Collections.emptyList());
            for (final var modifier : modifiers) {
                modifier.accept(node);
            }
            nodes.add(node);
            nodeResourceEventHandler.onAddSync(node);
        }
        return this;
    }

    TestResult runInitialPlacement() {
        assertNotNull(scheduler);
        return new TestResult(scheduler.initialPlacement(), nodes);
    }

    static PodGroup podGroup(final String... name) {
        return new PodGroup(name);
    }

    static NodesForPodGroup nodesForPodGroup(final String name) {
        return new NodesForPodGroup(name);
    }

    static NodeGroup nodeGroup(final String... names) {
        return new NodeGroup(names);
    }

    static class TestResult {
        private final Result<? extends Record> results;
        private final List<Node> nodes;

        private TestResult(final Result<? extends Record> result, final List<Node> nodes) {
            this.results = result;
            this.nodes = nodes;
        }

        public TestResult expect(final PodGroup leftGroup, final Op op, final PodGroup rightGroup) {
            assert op == Op.COLOCATED_WITH || op == Op.NOT_COLOCATED_WITH;
            assertEquals(1, leftGroup.names.size());
            results.stream()
                   .filter(r -> r.get("POD_NAME", String.class).startsWith(leftGroup.names.get(0) + "-"))
                   .forEach(leftGroupRow -> {
                            final String leftPodName = leftGroupRow.get("POD_NAME", String.class);
                            final String leftNodeName = leftGroupRow.get("CONTROLLABLE__NODE_NAME", String.class);
                            assertNotEquals("NULL_NODE", leftNodeName);
                            final Set<String> otherNodes = results.stream()
                                   .filter(e -> {
                                       for (final var groupName: rightGroup.names) {
                                           if (e.get("POD_NAME", String.class).startsWith(groupName + "-")) {
                                               return true;
                                           }
                                       }
                                       return false;
                                   })
                                   .filter(e -> !e.get("POD_NAME", String.class).equals(leftPodName))
                                   .map(e -> e.get("CONTROLLABLE__NODE_NAME", String.class))
                                   .collect(Collectors.toSet());
                            if (op == Op.COLOCATED_WITH) {
                                assertTrue(otherNodes.contains(leftNodeName), leftNodeName + " " + otherNodes);
                            } else {
                                assertFalse(otherNodes.contains(leftNodeName));
                            }
                       }
                   );
            return this;
        }

        public TestResult expect(final NodesForPodGroup podGroup, final Op op, final NodeGroup nodeGroup) {
            final Set<String> actual = results.stream()
                    .filter(e -> e.get("POD_NAME", String.class).startsWith(podGroup.name + "-"))
                    .map(e -> e.get("CONTROLLABLE__NODE_NAME", String.class))
                    .collect(Collectors.toSet());
            final Set<String> expected = nodes.stream()
                    .map(node -> node.getMetadata().getName())
                    .filter(nodeName -> {
                        for (final var groupName: nodeGroup.names) {
                            if (nodeName.startsWith(groupName + "-")) {
                                return true;
                            }
                        }
                        return false;
                    })
                    .collect(Collectors.toSet());
            if (nodeGroup.names.contains("NULL_NODE")) {
                expected.add("NULL_NODE");
            }
            switch (op) {
                case EQUALS:
                    assertEquals(expected, actual, String.format("Failed: %s == %s", actual, expected));
                    break;
                case IN:
                    assertTrue(expected.containsAll(actual),
                               String.format("Failed: %s IN %s", actual, expected));
                    break;
                case NOT_IN:
                    assertFalse(expected.containsAll(actual),
                                String.format("Failed: %s NOT IN %s", actual, expected));
                    break;
                default:
                    throw new IllegalArgumentException(op.toString());
            }
            return this;
        }
    }

    enum Op {
        NOT_COLOCATED_WITH,
        COLOCATED_WITH,
        EQUALS,
        IN,
        NOT_IN
    }

    static class PodGroup {
        private final List<String> names;

        PodGroup(final String... name) {
            assertNotEquals(0, name.length);
            this.names = Arrays.asList(name);
        }
    }

    static class NodesForPodGroup {
        private final String name;

        NodesForPodGroup(final String name) {
            this.name = name;
        }
    }

    static class NodeGroup {
        private final List<String> names;

        NodeGroup(final String... name) {
            assertNotEquals(0, name.length);
            this.names = Arrays.asList(name);
        }
    }
}