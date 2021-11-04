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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.vmware.dcm.SchedulerTest.newNode;
import static com.vmware.dcm.SchedulerTest.newPod;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A simple DSL to set up scheduling scenarios for testing
 */
class TestScenario {
    private static final Map<List<String>, Scheduler.Builder> BUILDER_CACHE = new HashMap<>();
    private final IConnectionPool dbConnectionPool;
    private final PodEventsToDatabase eventHandler;
    private final NodeResourceEventHandler nodeResourceEventHandler;
    private final PodResourceEventHandler podResourceEventHandler;
    private final Set<String> podGroups = new HashSet<>();
    private final Set<String> nodeGroups = new HashSet<>();
    private final List<Pod> pods = new ArrayList<>();
    private final List<Node> nodes = new ArrayList<>();
    private final Map<Node, Pod> systemPods = new HashMap<>();
    private final Scheduler.Builder schedulerBuilder;
    @Nullable private Scheduler scheduler;

    TestScenario() {
        this(new DBConnectionPool());
    }

    // Constructor to enable passing custom ConnectionPools, e.g. DDlogDBConnectionPool
    TestScenario (IConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
        this.schedulerBuilder = new Scheduler.Builder(dbConnectionPool);
        this.eventHandler = new PodEventsToDatabase(dbConnectionPool);
        this.nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        this.podResourceEventHandler = new PodResourceEventHandler(eventHandler::handle);
    }

    static TestScenario withPolicies(final List<String> initialPlacement, IConnectionPool connectionPool) {
        final var scenario = new TestScenario(connectionPool);
        scenario.schedulerBuilder.setInitialPlacementPolicies(initialPlacement);
        scenario.scheduler = scenario.schedulerBuilder.build();
        return scenario;
    }

    TestScenario(final DBConnectionPool pool) {
         this.dbConnectionPool = pool;
         this.eventHandler = new PodEventsToDatabase(dbConnectionPool);
         this.nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
         this.podResourceEventHandler = new PodResourceEventHandler(eventHandler::handle);
         this.schedulerBuilder = new Scheduler.Builder(dbConnectionPool);
    }

    TestScenario(final IConnectionPool pool, final Scheduler.Builder builder) {
        this.dbConnectionPool = pool;
        this.eventHandler = new PodEventsToDatabase(dbConnectionPool);
        this.nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        this.podResourceEventHandler = new PodResourceEventHandler(eventHandler::handle);
        this.schedulerBuilder = builder;
    }

    static TestScenario withPolicies(final List<String> initialPlacement) {
        // Re-use already compiled models across tests to reduce test time
        if (BUILDER_CACHE.containsKey(initialPlacement)) {
            final var schedulerBuilder = BUILDER_CACHE.get(initialPlacement);
            schedulerBuilder.connection.refresh();
            final var scenario = new TestScenario(schedulerBuilder.connection, schedulerBuilder);
            scenario.scheduler = schedulerBuilder.build();
            return scenario;
        }
        final var scenario = new TestScenario(new DBConnectionPool());
        scenario.schedulerBuilder.setInitialPlacementPolicies(initialPlacement);
        scenario.scheduler = scenario.schedulerBuilder.build();
        BUILDER_CACHE.put(initialPlacement, scenario.schedulerBuilder);
        return scenario;
    }

    static TestScenario withPolicies(final List<String> initialPlacement, final boolean scoped) {
        // Re-use already compiled models across tests to reduce test time
        if (BUILDER_CACHE.containsKey(initialPlacement)) {
            final var schedulerBuilder = BUILDER_CACHE.get(initialPlacement);
            schedulerBuilder.connection.refresh();
            schedulerBuilder.setScopedInitialPlacement(scoped);
            final var scenario = new TestScenario(schedulerBuilder.connection, schedulerBuilder);
            scenario.scheduler = schedulerBuilder.build();
            return scenario;
        }
        final var scenario = new TestScenario(new DBConnectionPool());
        scenario.schedulerBuilder.setInitialPlacementPolicies(initialPlacement);
        scenario.schedulerBuilder.setScopedInitialPlacement(scoped);
        scenario.scheduler = scenario.schedulerBuilder.build();
        BUILDER_CACHE.put(initialPlacement, scenario.schedulerBuilder);
        return scenario;
    }

    static TestScenario withPolicies(final List<String> initialPlacement, final boolean scoped, IConnectionPool dbConnectionPool) {
        final var scenario = new TestScenario(dbConnectionPool);
        scenario.schedulerBuilder.setInitialPlacementPolicies(initialPlacement);
        scenario.schedulerBuilder.setScopedInitialPlacement(scoped);
        scenario.scheduler = scenario.schedulerBuilder.build();
        return scenario;
    }

    static TestScenario withPolicies(final List<String> initialPlacement, final List<String> preemption) {
        final var scenario = new TestScenario(new DBConnectionPool());
        scenario.schedulerBuilder.setInitialPlacementPolicies(initialPlacement);
        scenario.schedulerBuilder.setPreemptionPolicies(preemption);
        scenario.scheduler = scenario.schedulerBuilder.build();
        return scenario;
    }

    /**
     * Adds a group of 'numPods' pods, who's names have the prefix 'groupName' to the test case.
     * A test can supply a 'modifier' lambdas to modify these pods as required for the test.
     */
    TestScenario withPodGroup(final String groupName, final int numPods, final Consumer<Pod>... modifiers) {
        if (podGroups.contains(groupName)) {
            throw new IllegalArgumentException("Pod group already exists: " + groupName);
        }
        podGroups.add(groupName);
        for (int i = 0; i < numPods; i++) {
            final Pod pod = newPod(groupName + "-" + i);
            for (final var modifier : modifiers) {
                modifier.accept(pod);
            }
            pods.add(pod);
        }
        return this;
    }

    /**
     * Adds a group of 'numNodes' nodes, who's names have the prefix 'groupName' to the test case.
     * A test can supply a 'modifier' lambdas to modify these nodes as required for the test.
     */
    TestScenario withNodeGroup(final String groupName, final int numNodes, final Consumer<Node>... modifiers) {
        if (nodeGroups.contains(groupName)) {
            throw new IllegalArgumentException("Node group already exists: " + groupName);
        }
        nodeGroups.add(groupName);
        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode(groupName + "-" + i,
                                      Collections.emptyMap(), Collections.emptyList());
            for (final var modifier : modifiers) {
                modifier.accept(node);
            }
            nodes.add(node);

            // Add one system pod per node
            final String podName = "system-pod-" + node.getMetadata().getName();
            final Pod pod;
            final String status = "Running";
            pod = newPod(podName, status);
            pod.getSpec().setNodeName(node.getMetadata().getName());
            systemPods.put(node, pod);
            pods.add(pod);
        }
        return this;
    }


    /**
     * Configure each system pod for a group of nodes
     */
    TestScenario forSystemPods(final String groupName, final Consumer<Pod>... modifiers) {
        if (!nodeGroups.contains(groupName)) {
            throw new IllegalArgumentException("Node group does not exist: " + groupName);
        }
        nodes.forEach(n -> Arrays.stream(modifiers).forEach(m -> m.accept(systemPods.get(n))));
        return this;
    }

    /**
     * Returns a scheduler if it has been instantiated
     */
    Scheduler scheduler() {
        return Objects.requireNonNull(scheduler);
    }

    /**
     * Returns a scheduler if it has been instantiated
     */
    IConnectionPool conn() {
        return dbConnectionPool;
    }

    /**
     * Adds all created pods/nodes to the test scenario.
     */
    TestScenario build() {
        Collections.shuffle(pods);
        Collections.shuffle(nodes);
        nodes.forEach(nodeResourceEventHandler::onAddSync);
        pods.forEach(podResourceEventHandler::onAddSync);
        return this;
    }

    /**
     * Runs initial placement using all configured pod and node groups. We shuffle the pod/nodes to avoid
     * artifacts from database insertion order in the test results.
     */
    TestResult runInitialPlacement() {
        build();
        assertNotNull(scheduler);
        return new TestResult(scheduler.initialPlacement(), nodes);
    }

    /**
     * Refers to a named group of pods
     */
    static PodGroup podGroup(final String... name) {
        return new PodGroup(name);
    }

    /**
     * Refers to the nodes that a named group of pods were assigned to
     */
    static NodesForPodGroup nodesForPodGroup(final String name) {
        return new NodesForPodGroup(name);
    }

    /**
     * Refers to a named group of nodes
     */
    static NodeGroup nodeGroup(final String... names) {
        return new NodeGroup(names);
    }

    /**
     * Encapsulates the results of running a DCM model. It includes the set of nodes that were configured
     * in the scenario and the ResultSet returned by DCM.
     */
    static class TestResult {
        final Result<? extends Record> results;
        private final List<Node> nodes;

        private TestResult(final Result<? extends Record> result, final List<Node> nodes) {
            this.results = result;
            this.nodes = nodes;
        }

        /**
         * For all nodes that a pod group was assigned to, check whether a predicate holds true (the argument
         * to the predicate is the list of node names).
         */
        public TestResult expect(final NodesForPodGroup podGroup, final Predicate<List<String>> predicate) {
            final Comparator<Record> comparator = Comparator.comparingInt(value -> {
                final String podIdx = value.get("POD_NAME", String.class).split("-")[1];
                return Integer.parseInt(podIdx);
            });
            final List<String> collect = results.stream()
                    .filter(e -> e.get("POD_NAME", String.class).startsWith(podGroup.name + "-"))
                    .sorted(comparator)
                    .map(e -> e.get("CONTROLLABLE__NODE_NAME", String.class))
                    .collect(Collectors.toList());
            assertTrue(predicate.test(collect));
            return this;
        }

        /**
         * Overload to check whether two groups of pods end up co-located with each or not
         */
        public TestResult expect(final PodGroup leftGroup, final Op op, final PodGroup rightGroup) {
            assert op == Op.COLOCATED_WITH || op == Op.NOT_COLOCATED_WITH;
            assertEquals(1, leftGroup.names.size());
            results.stream()
                   .filter(r -> r.get("POD_NAME", String.class).startsWith(leftGroup.names.get(0) + "-"))
                   .forEach(leftGroupRow -> {
                            final String leftPodName = leftGroupRow.get("POD_NAME", String.class);
                            final String leftNodeName = leftGroupRow.get("CONTROLLABLE__NODE_NAME", String.class);
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

        /**
         * Overload to compare nodes assigned to a group of pods with a given group of nodes
         */
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
                case EQUALS -> assertEquals(expected, actual, String.format("Failed: %s == %s", actual, expected));
                case NOT_EQUALS -> assertNotEquals(expected, actual,
                                                   String.format("Failed: %s != %s", actual, expected));
                case IN -> assertTrue(expected.containsAll(actual),
                        String.format("Failed: %s IN %s", actual, expected));
                case NOT_IN -> assertFalse(expected.containsAll(actual),
                        String.format("Failed: %s NOT IN %s", actual, expected));
                default -> throw new IllegalArgumentException(op.toString());
            }
            return this;
        }
    }

    enum Op {
        NOT_COLOCATED_WITH,
        COLOCATED_WITH,
        EQUALS,
        NOT_EQUALS,
        IN,
        NOT_IN
    }

    private static class PodGroup {
        private final List<String> names;

        PodGroup(final String... name) {
            assertNotEquals(0, name.length);
            this.names = Arrays.asList(name);
        }
    }

    private static class NodesForPodGroup {
        private final String name;

        NodesForPodGroup(final String name) {
            this.name = name;
        }
    }

    private static class NodeGroup {
        private final List<String> names;

        NodeGroup(final String... name) {
            assertNotEquals(0, name.length);
            this.names = Arrays.asList(name);
        }
    }
}
