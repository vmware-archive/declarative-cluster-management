/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.collect.Sets;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Affinity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1LabelSelectorBuilder;
import io.kubernetes.client.models.V1LabelSelectorRequirement;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeAffinity;
import io.kubernetes.client.models.V1NodeCondition;
import io.kubernetes.client.models.V1NodeSelector;
import io.kubernetes.client.models.V1NodeSelectorBuilder;
import io.kubernetes.client.models.V1NodeSelectorRequirement;
import io.kubernetes.client.models.V1NodeSelectorTerm;
import io.kubernetes.client.models.V1NodeSelectorTermBuilder;
import io.kubernetes.client.models.V1NodeSpec;
import io.kubernetes.client.models.V1NodeStatus;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodAffinity;
import io.kubernetes.client.models.V1PodAffinityTerm;
import io.kubernetes.client.models.V1PodAffinityTermBuilder;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.reactivex.processors.PublishProcessor;
import org.dcm.k8s.generated.Tables;
import org.joda.time.DateTime;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the scheduler
 */
public class SchedulerTest {
    /*
     * Double checks that delete cascades work.
     */
    @Test
    public void testDeleteCascade() {
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        final String podName = "p1";
        final V1Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.singletonMap("k", "v"));
        handler.onAdd(pod);
        assertTrue(conn.fetchExists(Tables.POD_INFO));
        assertTrue(conn.fetchExists(Tables.POD_LABELS));
        handler.onDelete(pod, true);
        assertFalse(conn.fetchExists(Tables.POD_INFO));
        assertFalse(conn.fetchExists(Tables.POD_LABELS));
    }

    /*
     * Since we use strings to encode policy names, double check that we haven't added duplicate names.
     */
    @Test
    public void testNoDuplicatePolicyNames() {
        final List<String> namesList = Policies.getAllPolicies();
        final Set<String> namesSet = new HashSet<>(Policies.getAllPolicies());
        assertEquals(namesList.size(), namesSet.size());
    }

    /*
     * Evaluates the node predicates policy. One randomly chosen node is free of any node conditions, so all pod
     * assignments must go to that node.
     */
    @ParameterizedTest
    @MethodSource("conditions")
    public void testSchedulerNodePredicates(final String type, final String status) {
        final DSLContext conn = Scheduler.setupDb();
        final List<String> policies = Policies.from(Policies.nodePredicates());
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        final int numNodes = 5;
        final int numPods = 10;
        conn.insertInto(Tables.BATCH_SIZE).values(numPods).execute();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        // We pick a random node from [0, numNodes) to assign all pods to.
        final int nodeToAssignTo = ThreadLocalRandom.current().nextInt(numNodes);
        for (int i = 0; i < numNodes; i++) {
            final V1NodeCondition badCondition = new V1NodeCondition();
            badCondition.setStatus(status);
            badCondition.setType(type);
            nodeResourceEventHandler.onAdd(addNode("n" + i, Collections.emptyMap(),
                                           i == nodeToAssignTo ? Collections.emptyList() : List.of(badCondition)));
        }

        for (int i = 0; i < numPods; i++) {
            handler.onAdd(newPod("p" + i));
        }

        // All pod additions have completed
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");
        final Result<? extends Record> results = scheduler.runOneLoop();
        assertEquals(numPods, results.size());
        results.forEach(r -> assertEquals("n" + nodeToAssignTo, r.get("CONTROLLABLE__NODE_NAME", String.class)));
    }

    @SuppressWarnings("UnusedMethod")
    private static Stream conditions() {
        return Stream.of(Arguments.of("OutOfDisk", "True"),
                         Arguments.of("MemoryPressure", "True"),
                         Arguments.of("DiskPressure", "True"),
                         Arguments.of("PIDPressure", "True"),
                         Arguments.of("NetworkUnavailable", "True"),
                         Arguments.of("Ready", "False"));
    }

    /*
     * Tests the pod_node_selector_matches view.
     */
    @ParameterizedTest
    @MethodSource("nodeSelectorConditions")
    public void testPodNodeSelector(final Set<String> podsToMatch, final Set<String> podsPartialMatch,
                                    final Set<String> nodesToMatch, final Set<String> nodesPartialMatch) {
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        final int numPods = 10;
        final int numNodes = 10;

        conn.insertInto(Tables.BATCH_SIZE).values(numPods).execute();

        // Add all pods, some of which have both the disk and gpu node selectors, whereas others only have the disk
        // node selector
        final Set<String> podsWithoutLabels = new HashSet<>();
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Map<String, String> selectorLabels = new HashMap<>();
            if (podsToMatch.contains(podName)) {
                selectorLabels.put("diskType", "ssd");
                selectorLabels.put("gpu", "true");
            }
            else if (podsPartialMatch.contains(podName)) {
                selectorLabels.put("diskType", "ssd");
            } else {
                podsWithoutLabels.add(podName);
            }
            handler.onAdd(newPod(podName, "Pending", selectorLabels, Collections.emptyMap()));
        }

        // Add all nodes, some of which have both the disk and gpu labels, whereas others only have the disk label
        final Set<String> nodesWithoutLabels = new HashSet<>();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Map<String, String> nodeLabels = new HashMap<>();
            if (nodesToMatch.contains(nodeName)) {
                nodeLabels.put("diskType", "ssd");
                nodeLabels.put("gpu", "true");
            }
            else if (nodesPartialMatch.contains(nodeName)) {
                nodeLabels.put("diskType", "ssd");
            }
            else {
                nodesWithoutLabels.add(nodeName);
            }
            nodeResourceEventHandler.onAdd(addNode(nodeName, nodeLabels, Collections.emptyList()));
        }

        // First, we check if the computed intermediate view is correct
        final Map<String, List<String>> podsToNodesMap = conn.selectFrom(Tables.POD_NODE_SELECTOR_MATCHES)
                                                              .fetchGroups(Tables.POD_NODE_SELECTOR_MATCHES.POD_NAME,
                                                                           Tables.POD_NODE_SELECTOR_MATCHES.NODE_NAME);
        podsToMatch.forEach(p -> assertTrue(podsToNodesMap.containsKey(p)));
        podsPartialMatch.forEach(p -> assertTrue(podsToNodesMap.containsKey(p)));
        podsWithoutLabels.forEach(p -> assertFalse(podsToNodesMap.containsKey(p)));
        for (final Map.Entry<String, List<String>> record: podsToNodesMap.entrySet()) {
            final String pod = record.getKey();
            final Set<String> nodes = new HashSet<>(record.getValue());
            if (podsToMatch.contains(pod)) {
                assertEquals(nodesToMatch, nodes);
            } else if (podsPartialMatch.contains(pod)) {
                assertEquals(Sets.union(nodesToMatch, nodesPartialMatch), nodes);
            } else {
                assert false : "Cannot happen";
            }
        }

        // Now test the solver itself
        final List<String> policies = Policies.from(Policies.nodePredicates(), Policies.nodeSelectorPredicate());

        // Chuffed does not work on Minizinc 2.3.0: https://github.com/MiniZinc/libminizinc/issues/321
        // Works when using Minizinc 2.3.2
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");
        final Result<? extends Record> results = scheduler.runOneLoop();
        assertEquals(numPods, results.size());
        results.forEach(r -> {
            final String pod = r.get("POD_NAME", String.class);
            final String node = r.get("CONTROLLABLE__NODE_NAME", String.class);
            if (podsToMatch.contains(pod)) {
                assertTrue(nodesToMatch.contains(node), String.format("%s assigned to %s", pod, node));
            } else if (podsPartialMatch.contains(pod)) {
                assertTrue(nodesToMatch.contains(node) || nodesPartialMatch.contains(node),
                           String.format("%s assigned to %s", pod, node));
            } else {
                assertTrue(nodesToMatch.contains(node)
                        || nodesPartialMatch.contains(node)
                        || nodesWithoutLabels.contains(node),
                        String.format("%s assigned to %s", pod, node));
            }
        });
    }

    @SuppressWarnings("UnusedMethod")
    private static Stream nodeSelectorConditions() {
        return Stream.of(Arguments.of(Set.of("p2", "p4"), Set.of("p6"), Set.of("n4"), Set.of("n5")),
                         Arguments.of(Set.of(), Set.of("p6"), Set.of(), Set.of("n5")));
    }


    /*
     * Tests the pod_node_selector_matches view.
     */
    @ParameterizedTest
    @MethodSource("testNodeAffinity")
    public void testPodToNodeAffinity(final List<V1NodeSelectorTerm> terms, final Map<String, String> nodeLabelsInput,
                                      final boolean shouldBeAffineToLabelledNodes,
                                      final boolean shouldBeAffineToRemainingNodes) {
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        final int numPods = 10;
        final int numNodes = 100;
        final int numPodsToModify = 3;
        final int numNodesToModify = 20;

        // Add all pods, some of which have both the disk and gpu node selectors, whereas others only have the disk
        // node selector
        final Set<String> podsToAssign = ThreadLocalRandom.current().ints(numPodsToModify, 0, numPods)
                                                           .mapToObj(i -> "p" + i)
                                                           .collect(Collectors.toSet());
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final V1Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.emptyMap());
            if (podsToAssign.contains(podName)) {
                final V1NodeSelector selector = new V1NodeSelectorBuilder()
                                                            .withNodeSelectorTerms(terms)
                                                            .build();
                pod.getSpec().getAffinity().getNodeAffinity()
                   .setRequiredDuringSchedulingIgnoredDuringExecution(selector);
            }
            handler.onAdd(pod);
        }

        // Add all nodes, some of which have labels described by nodeLabelsInput,
        // whereas others have a different set of labels
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        final Set<String> nodesToAssign = ThreadLocalRandom.current()
                                                         .ints(numNodesToModify, 0, numNodes)
                                                         .mapToObj(i -> "n" + i)
                                                         .collect(Collectors.toSet());
        final Set<String> remainingNodes = new HashSet<>();
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Map<String, String> nodeLabels = new HashMap<>();
            if (nodesToAssign.contains(nodeName)) {
                nodeLabels.putAll(nodeLabelsInput);
            } else {
                nodeLabels.put("dummyKey1", "dummyValue1");
                nodeLabels.put("dummyKey2", "dummyValue2");
                remainingNodes.add(nodeName);
            }
            nodeResourceEventHandler.onAdd(addNode(nodeName, nodeLabels, Collections.emptyList()));
        }

        // First, we check if the computed intermediate views are correct
        final Map<String, List<String>> podsToNodesMap = conn.selectFrom(Tables.POD_NODE_SELECTOR_MATCHES)
                                                             .fetchGroups(Tables.POD_NODE_SELECTOR_MATCHES.POD_NAME,
                                                                          Tables.POD_NODE_SELECTOR_MATCHES.NODE_NAME);
        podsToAssign.forEach(p -> assertEquals(podsToNodesMap.containsKey(p),
                                               shouldBeAffineToLabelledNodes || shouldBeAffineToRemainingNodes));
        podsToNodesMap.forEach(
            (pod, nodeList) -> {
                assertTrue(podsToAssign.contains(pod));
                nodeList.forEach(
                    node -> {
                        if (shouldBeAffineToLabelledNodes && shouldBeAffineToRemainingNodes) {
                            assertTrue(nodesToAssign.contains(node) || remainingNodes.contains(node));
                        }
                        else if (shouldBeAffineToLabelledNodes) {
                            assertTrue(nodesToAssign.contains(node));
                        }
                        else if (shouldBeAffineToRemainingNodes) {
                            assertFalse(nodesToAssign.contains(node));
                        }
                    }
                );
            }
        );

        assertEquals(Sets.newHashSet(conn.selectFrom(Tables.POD_NODE_SELECTOR_LABELS)
                                .fetch("POD_NAME")),
                     Sets.newHashSet(conn.selectFrom(Tables.POD_INFO)
                                .where(Tables.POD_INFO.HAS_NODE_SELECTOR_LABELS.eq(true))
                                .fetch("POD_NAME")));

        // Now test the solver itself
        final List<String> policies = Policies.from(Policies.nodePredicates(), Policies.nodeSelectorPredicate());

        // Note: Chuffed does not work on Minizinc 2.3.0: https://github.com/MiniZinc/libminizinc/issues/321
        // but works when using Minizinc 2.3.2
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");

        if (!shouldBeAffineToLabelledNodes && !shouldBeAffineToRemainingNodes) {
            // Should be unsat
            assertThrows(ModelException.class, scheduler::runOneLoop);
        } else {
            final Result<? extends Record> results = scheduler.runOneLoop();
            assertEquals(numPods, results.size());
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static Stream testNodeAffinity() {
        final List<V1NodeSelectorTerm> inTerm = List.of(term(nodeExpr("k1", "In", "l1", "l2")));
        final List<V1NodeSelectorTerm> existsTerm = List.of(term(nodeExpr("k1", "Exists", "l1", "l2")));
        final List<V1NodeSelectorTerm> notInTerm = List.of(term(nodeExpr("k1", "NotIn", "l1", "l2")));
        final List<V1NodeSelectorTerm> notExistsTerm = List.of(term(nodeExpr("k1", "DoesNotExist", "l1", "l2")));
        return Stream.of(
                // First, we test to see if all our operators work on their own

                // In
                Arguments.of(inTerm, map("k1", "l1"), true, false),
                Arguments.of(inTerm, map("k1", "l2"), true, false),
                Arguments.of(inTerm, map("k1", "l3"), false, false),
                Arguments.of(inTerm, map("k", "l", "k1", "l1"), true, false),
                Arguments.of(inTerm, map("k", "l", "k1", "l2"), true, false),
                Arguments.of(inTerm, map("k", "l", "k1", "l3"), false, false),

                // Exists
                Arguments.of(existsTerm, map("k1", "l1"), true, false),
                Arguments.of(existsTerm, map("k1", "l2"), true, false),
                Arguments.of(existsTerm, map("k1", "l3"), true, false),
                Arguments.of(existsTerm, map("k2", "l3"), false, false),
                Arguments.of(existsTerm, map("k2", "l1"), false, false),
                Arguments.of(existsTerm, map("k", "l", "k1", "l1"), true, false),
                Arguments.of(existsTerm, map("k", "l", "k1", "l2"), true, false),
                Arguments.of(existsTerm, map("k", "l", "k1", "l3"), true, false),
                Arguments.of(existsTerm, map("k", "l", "k2", "l1"), false, false),
                Arguments.of(existsTerm, map("k", "l", "k2", "l2"), false, false),
                Arguments.of(existsTerm, map("k", "l", "k2", "l3"), false, false),

                // NotIn
                Arguments.of(notInTerm, map("k1", "l1"), false, true),
                Arguments.of(notInTerm, map("k1", "l2"), false, true),
                Arguments.of(notInTerm, map("k1", "l3"), true, true),
                Arguments.of(notInTerm, map("k", "l", "k1", "l1"), false, true),
                Arguments.of(notInTerm, map("k", "l", "k1", "l2"), false, true),
                Arguments.of(notInTerm, map("k", "l", "k1", "l3"), true, true),

                // DoesNotExist
                Arguments.of(notExistsTerm, map("k1", "l1"), false, true),
                Arguments.of(notExistsTerm, map("k1", "l2"), false, true),
                Arguments.of(notExistsTerm, map("k1", "l3"), false, true),
                Arguments.of(notExistsTerm, map("k", "l", "k1", "l1"), false, true),
                Arguments.of(notExistsTerm, map("k", "l", "k1", "l2"), false, true),
                Arguments.of(notExistsTerm, map("k", "l", "k1", "l3"), false, true)
        );
    }


    /*
     * Tests inter-pod affinity behavior
     */
    @ParameterizedTest
    @MethodSource("testPodAffinity")
    public void testPodToPodAffinity(final List<V1PodAffinityTerm> terms, final Map<String, String> podLabelsInput,
                                     final boolean shouldBeAffineToLabelledPods,
                                     final boolean shouldBeAffineToRemainingPods,
                                     final boolean cannotBePlacedAnywhere) {
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        final int numPods = 10;
        final int numNodes = 10;
        final int numPodsToModify = 3;

        // Add all pods, some of which have both the disk and gpu node selectors, whereas others only have the disk
        // node selector
        final Set<String> podsToAssign = ThreadLocalRandom.current().ints(numPodsToModify, 0, numPods)
                                                .mapToObj(i -> "p" + i)
                                                .collect(Collectors.toSet());

        // If we only get one pod in podsToAssign, then that will be the only labelled pod. In cases of affinity
        // requirements, that means that that pod will not have any candidate nodes to be placed on.
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final V1Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.emptyMap());
            if (podsToAssign.contains(podName)) {
                final V1PodAffinity podAffinity = new V1PodAffinity();
                terms.forEach(podAffinity::addRequiredDuringSchedulingIgnoredDuringExecutionItem);
                pod.getMetadata().setLabels(podLabelsInput);
                pod.getSpec().getAffinity().setPodAffinity(podAffinity);
            } else {
                pod.getMetadata().setLabels(Collections.singletonMap("dummyKey", "dummyValue"));
            }
            handler.onAdd(pod);
        }

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            nodeResourceEventHandler.onAdd(addNode(nodeName, Collections.emptyMap(), Collections.emptyList()));
        }

        final List<String> policies = Policies.from(Policies.nodePredicates(), Policies.podAffinityPredicate());
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");
        if (cannotBePlacedAnywhere) {
            assertThrows(ModelException.class, scheduler::runOneLoop);
        } else {
            final Result<? extends Record> result = scheduler.runOneLoop();
            for (final Record record: result) {
                final String podName = record.getValue("POD_NAME", String.class);
                final String assignedNode = record.getValue("CONTROLLABLE__NODE_NAME", String.class);
                final Set<String> nodesAssignedToPodsWithAffinityRequirements = result.stream()
                        .filter(e -> podsToAssign.contains(e.getValue("POD_NAME", String.class)))
                        .filter(e -> podsToAssign.size() == 1 || !podName.equals(e.getValue("POD_NAME", String.class)))
                        .map(e -> e.getValue("CONTROLLABLE__NODE_NAME", String.class))
                        .collect(Collectors.toSet());
                final Set<String> nodesAssignedToPodsWithoutAffinityRequirements = result.stream()
                        .filter(e -> !podsToAssign.contains(e.getValue("POD_NAME", String.class)))
                        .filter(e -> podsToAssign.size() == 1 || !podName.equals(e.getValue("POD_NAME", String.class)))
                        .map(e -> e.getValue("CONTROLLABLE__NODE_NAME", String.class))
                        .collect(Collectors.toSet());
                if (podsToAssign.contains(podName) && shouldBeAffineToLabelledPods && shouldBeAffineToRemainingPods) {
                    assertTrue(nodesAssignedToPodsWithAffinityRequirements.contains(assignedNode) ||
                               nodesAssignedToPodsWithoutAffinityRequirements.contains(assignedNode));
                } else if (podsToAssign.contains(podName) && shouldBeAffineToLabelledPods) {
                    assertTrue(nodesAssignedToPodsWithAffinityRequirements.contains(assignedNode));
                } else if (podsToAssign.contains(podName) && shouldBeAffineToRemainingPods) {
                    assertTrue(nodesAssignedToPodsWithoutAffinityRequirements.contains(assignedNode));
                }
            }
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static Stream testPodAffinity() {
        final String topologyKey = "kubernetes.io/hostname";
        final List<V1PodAffinityTerm> inTerm = List.of(term(topologyKey,
                                                       podExpr("k1", "In", "l1", "l2")));
        final List<V1PodAffinityTerm> existsTerm = List.of(term(topologyKey,
                                                                podExpr("k1", "Exists", "l1", "l2")));
        final List<V1PodAffinityTerm> notInTerm = List.of(term(topologyKey,
                                                          podExpr("k1", "NotIn", "l1", "l2")));
        final List<V1PodAffinityTerm> notExistsTerm = List.of(term(topologyKey,
                                                                   podExpr("k1", "DoesNotExist", "l1", "l2")));
        return Stream.of(
                // First, we test to see if all our operators work on their own

                // In
                Arguments.of(inTerm, map("k1", "l1"), true, false, false),
                Arguments.of(inTerm, map("k1", "l2"), true, false, false),
                Arguments.of(inTerm, map("k1", "l3"), false, false, true),
                Arguments.of(inTerm, map("k", "l", "k1", "l1"), true, false, false),
                Arguments.of(inTerm, map("k", "l", "k1", "l2"), true, false, false),
                Arguments.of(inTerm, map("k", "l", "k1", "l3"), false, false, true),

                // Exists
                Arguments.of(existsTerm, map("k1", "l1"), true, false, false),
                Arguments.of(existsTerm, map("k1", "l2"), true, false, false),
                Arguments.of(existsTerm, map("k1", "l3"), true, false, false),
                Arguments.of(existsTerm, map("k2", "l3"), false, false, true),
                Arguments.of(existsTerm, map("k2", "l1"), false, false, true),
                Arguments.of(existsTerm, map("k", "l", "k1", "l1"), true, false, false),
                Arguments.of(existsTerm, map("k", "l", "k1", "l2"), true, false, false),
                Arguments.of(existsTerm, map("k", "l", "k1", "l3"), true, false, false),
                Arguments.of(existsTerm, map("k", "l", "k2", "l1"), false, false, true),
                Arguments.of(existsTerm, map("k", "l", "k2", "l2"), false, false, true),
                Arguments.of(existsTerm, map("k", "l", "k2", "l3"), false, false, true),

                // NotIn
                Arguments.of(notInTerm, map("k1", "l1"), false, true, false),
                Arguments.of(notInTerm, map("k1", "l2"), false, true, false),
                Arguments.of(notInTerm, map("k1", "l3"), true, true, false),
                Arguments.of(notInTerm, map("k", "l", "k1", "l1"), false, true, false),
                Arguments.of(notInTerm, map("k", "l", "k1", "l2"), false, true, false),
                Arguments.of(notInTerm, map("k", "l", "k1", "l3"), true, true, false),

                // DoesNotExist
                Arguments.of(notExistsTerm, map("k1", "l1"), false, true, false),
                Arguments.of(notExistsTerm, map("k1", "l2"), false, true, false),
                Arguments.of(notExistsTerm, map("k1", "l3"), false, true, false),
                Arguments.of(notExistsTerm, map("k", "l", "k1", "l1"), false, true, false),
                Arguments.of(notExistsTerm, map("k", "l", "k1", "l2"), false, true, false),
                Arguments.of(notExistsTerm, map("k", "l", "k1", "l3"), false, true, false)
        );
    }


    /*
     * Capacity constraints
     */
    @ParameterizedTest(name = "{0} => feasible:{6}")
    @MethodSource("spareCapacityValues")
    public void testSpareCapacity(final String displayName, final List<Integer> cpuRequests,
                                  final List<Integer> memoryRequests, final List<Integer> nodeCpuCapacities,
                                  final List<Integer> nodeMemoryCapacities,
                                  final Set<String> nodesThatShouldNotHavePods, final boolean feasible) {
        assertEquals(cpuRequests.size(), memoryRequests.size());
        assertEquals(nodeCpuCapacities.size(), nodeMemoryCapacities.size());
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();
        final int numPods = cpuRequests.size();
        final int numNodes = nodeCpuCapacities.size();

        // Add pending pods
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final V1Pod pod;

            final Map<String, Quantity> resourceRequests = new HashMap<>();
            resourceRequests.put("cpu", new Quantity(String.valueOf(cpuRequests.get(i))));
            resourceRequests.put("memory", new Quantity(String.valueOf(memoryRequests.get(i))));
            pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.emptyMap());

            // Assumes that there is only one container
            pod.getSpec().getContainers().get(0)
                .getResources()
                .setRequests(resourceRequests);
            handler.onAdd(pod);
        }

        // Add all nodes and one system pod per node
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final V1Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(nodeCpuCapacities.get(i))));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(nodeMemoryCapacities.get(i))));
            nodeResourceEventHandler.onAdd(node);

            // Add one system pod per node
            final String podName = "system-pod-" + nodeName;
            final V1Pod pod;
            final String status = "Running";
            pod = newPod(podName, status, Collections.emptyMap(), Collections.emptyMap());
            pod.getSpec().setNodeName(nodeName);
            handler.onAdd(pod);
        }

        final List<String> policies = Policies.from(Policies.nodePredicates(), Policies.capacityConstraint());
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");
        if (feasible) {
            final Result<? extends Record> result = scheduler.runOneLoop();
            assertEquals(numNodes, result.size());
            final Set<String> nodes = result.stream()
                                            .map(e -> e.getValue("CONTROLLABLE__NODE_NAME", String.class))
                                            .collect(Collectors.toSet());
            nodes.forEach(e -> assertFalse(nodesThatShouldNotHavePods.contains(e)));
        } else {
            assertThrows(ModelException.class, scheduler::runOneLoop);
        }
    }


    @SuppressWarnings("UnusedMethod")
    private static Stream spareCapacityValues() {
        return Stream.of(
                Arguments.of("One pod per node",
                             List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                             List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                             Set.of(), true),

                Arguments.of("p1 cannot be placed",
                        List.of(10, 11, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        Set.of(), false),

                Arguments.of("n2 does not have sufficient CPU capacity and should not host any new pods",
                        List.of(5, 5, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        List.of(10, 10, 1, 10, 10), List.of(20, 20, 20, 20, 20),
                        Set.of("n2"), true),

                Arguments.of("Only memory requests, and all pods must go to n3",
                        List.of(0, 0, 0, 0, 0), List.of(10, 10, 10, 10, 10),
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 50, 0),
                        Set.of("n0", "n1", "n2", "n4"), true),

                Arguments.of("No resources requests => pods can go to any node",
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 0, 0),
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 0, 0),
                        Set.of(), true)
        );
    }

    private static Map<String, String> map(final String k1, final String v1) {
        return Collections.singletonMap(k1, v1);
    }

    private static Map<String, String> map(final String k1, final String v1, final String k2, final String v2) {
        final Map<String, String> ret = new HashMap<>();
        ret.put(k1, v1);
        ret.put(k2, v2);
        return ret;
    }

    private static V1NodeSelectorTerm term(final V1NodeSelectorRequirement... requirements) {
        return new V1NodeSelectorTermBuilder()
                   .withMatchExpressions(requirements)
                   .build();
    }

    private static V1NodeSelectorRequirement nodeExpr(final String key, final String op, final String... values) {
        final V1NodeSelectorRequirement requirement = new V1NodeSelectorRequirement();
        requirement.setKey(key);
        requirement.setOperator(op);
        requirement.setValues(List.of(values));
        return requirement;
    }

    private static V1PodAffinityTerm term(final String topologyKey, final V1LabelSelectorRequirement... requirements) {
        final V1LabelSelector labelSelector = new V1LabelSelectorBuilder()
                .withMatchExpressions(requirements)
                .build();
        return new V1PodAffinityTermBuilder()
                      .withLabelSelector(labelSelector)
                      .withTopologyKey(topologyKey)
                      .build();
    }

    private static V1LabelSelectorRequirement podExpr(final String key, final String op, final String... values) {
        final V1LabelSelectorRequirement requirement = new V1LabelSelectorRequirement();
        requirement.setKey(key);
        requirement.setOperator(op);
        requirement.setValues(List.of(values));
        return requirement;
    }

    private V1Pod newPod(final String name) {
        return newPod(name, "Pending", Collections.emptyMap(), Collections.emptyMap());
    }

    private V1Pod newPod(final String podName, final String phase, final Map<String, String> selectorLabels,
                         final Map<String, String> labels) {
        final V1Pod pod = new V1Pod();
        final V1ObjectMeta meta = new V1ObjectMeta();
        final DateTime dateTime = new DateTime();
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp(dateTime);
        meta.setNamespace("default");
        final V1PodSpec spec = new V1PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        spec.nodeSelector(selectorLabels);

        final V1Container container = new V1Container();
        container.setName("pause");

        final V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
        resourceRequirements.setRequests(Collections.emptyMap());
        container.setResources(resourceRequirements);
        spec.addContainersItem(container);

        final V1Affinity affinity = new V1Affinity();
        final V1NodeAffinity nodeAffinity = new V1NodeAffinity();
        affinity.setNodeAffinity(nodeAffinity);
        spec.setAffinity(affinity);
        final V1PodStatus status = new V1PodStatus();
        status.setPhase(phase);
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }

    private V1Node addNode(final String nodeName, final Map<String, String> labels,
                           final List<V1NodeCondition> conditions) {
        final V1Node node = new V1Node();
        final V1NodeStatus status = new V1NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("10"));
        quantityMap.put("memory", new Quantity("1000"));
        quantityMap.put("ephemeral-storage", new Quantity("1000"));
        quantityMap.put("pods", new Quantity("100"));
        status.setCapacity(quantityMap);
        status.setAllocatable(quantityMap);
        status.setImages(Collections.emptyList());
        node.setStatus(status);
        status.setConditions(conditions);
        final V1NodeSpec spec = new V1NodeSpec();
        spec.setUnschedulable(false);
        node.setSpec(spec);
        final V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(nodeName);
        meta.setLabels(labels);
        node.setMetadata(meta);
        return node;
    }
}