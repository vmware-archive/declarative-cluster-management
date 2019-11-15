/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.collect.Sets;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeSelector;
import io.fabric8.kubernetes.api.model.NodeSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinity;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodAffinityTermBuilder;
import io.fabric8.kubernetes.api.model.PodAntiAffinity;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.api.model.Toleration;
import io.reactivex.processors.PublishProcessor;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for the scheduler
 */
@ExtendWith({})
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
        final Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.singletonMap("k", "v"));
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
            final NodeCondition badCondition = new NodeCondition();
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
    public void testPodToNodeAffinity(final List<NodeSelectorTerm> terms, final Map<String, String> nodeLabelsInput,
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

        final List<String> allPods = IntStream.range(0, numPods)
                .mapToObj(i -> "p" + i)
                .collect(Collectors.toList());
        Collections.shuffle(allPods);
        final Set<String> podsToAssign = new HashSet<>(allPods.subList(0, numPodsToModify));

        // Add all pods
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.emptyMap());
            if (podsToAssign.contains(podName)) {
                final NodeSelector selector = new NodeSelectorBuilder()
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
        final List<NodeSelectorTerm> inTerm = List.of(term(nodeExpr("k1", "In", "l1", "l2")));
        final List<NodeSelectorTerm> existsTerm = List.of(term(nodeExpr("k1", "Exists", "l1", "l2")));
        final List<NodeSelectorTerm> notInTerm = List.of(term(nodeExpr("k1", "NotIn", "l1", "l2")));
        final List<NodeSelectorTerm> notExistsTerm = List.of(term(nodeExpr("k1", "DoesNotExist", "l1", "l2")));
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
    @ParameterizedTest(name = "{0}")
    @MethodSource("testPodAffinity")
    public void testPodToPodAffinityOrAntiAffinity(final String label, final String condition,
                                                   final List<PodAffinityTerm> terms,
                                                   final Map<String, String> podLabelsInput,
                                                   final boolean conditionToLabelledPods,
                                                   final boolean conditionToRemainingPods,
                                                   final boolean cannotBePlacedAnywhere) {
        final DSLContext conn = Scheduler.setupDb();
        final int numPods = 10;
        final int numPodsToModify = 3;
        final int numNodes = 10;

        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        final List<String> allPods = IntStream.range(0, numPods)
                .mapToObj(i -> "p" + i)
                .collect(Collectors.toList());
        Collections.shuffle(allPods);
        final Set<String> podsToAssign = new HashSet<>(allPods.subList(0, numPodsToModify));

        // If we only get one pod in podsToAssign, then that will be the only labelled pod. In cases of affinity
        // requirements, that means that that pod will not have any candidate nodes to be placed on.
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.emptyMap());
            if (podsToAssign.contains(podName)) {
                pod.getMetadata().setLabels(podLabelsInput);

                if (condition.equals("AntiAffinity")) {
                    final PodAntiAffinity podAntiAffinity = new PodAntiAffinity();
                    final List<PodAffinityTerm> podAntiAffinityTerms =
                            podAntiAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
                    podAntiAffinityTerms.addAll(terms);
                    pod.getSpec().getAffinity().setPodAntiAffinity(podAntiAffinity);
                } else if (condition.equals("Affinity")) {
                    final PodAffinity podAffinity = new PodAffinity();
                    final List<PodAffinityTerm> podAffinityTerms =
                            podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
                    podAffinityTerms.addAll(terms);
                    pod.getSpec().getAffinity().setPodAffinity(podAffinity);
                } else {
                    throw new IllegalArgumentException(condition);
                }
            } else {
                pod.getMetadata().setLabels(Collections.singletonMap("dummyKey", "dummyValue"));
            }
            handler.onAdd(pod);
        }

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            nodeResourceEventHandler.onAdd(node);
        }

        final List<String> policies = Policies.from(Policies.nodePredicates(),
                                                    Policies.podAffinityPredicate(),
                                                    Policies.podAntiAffinityPredicate());
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

                if (condition.equals("Affinity")) {
                    // conditionToLabelledPods => affineToLabelledPods
                    // conditionToRemainingPods => affineToRemainingPods
                    if (podsToAssign.contains(podName) && conditionToLabelledPods && conditionToRemainingPods) {
                        assertTrue(nodesAssignedToPodsWithAffinityRequirements.contains(assignedNode) ||
                                nodesAssignedToPodsWithoutAffinityRequirements.contains(assignedNode));
                    } else if (podsToAssign.contains(podName) && conditionToLabelledPods) {
                        assertTrue(nodesAssignedToPodsWithAffinityRequirements.contains(assignedNode));
                    } else if (podsToAssign.contains(podName) && conditionToRemainingPods) {
                        assertTrue(nodesAssignedToPodsWithoutAffinityRequirements.contains(assignedNode));
                    }
                } else if (condition.equals("AntiAffinity")) {
                    // conditionToLabelledPods => antiAffineToLabelledPods
                    // conditionToRemainingPods => antiAffineToRemainingPods
                    if (podsToAssign.contains(podName) && conditionToLabelledPods && conditionToRemainingPods) {
                        fail();
                    } else if (podsToAssign.contains(podName) && conditionToLabelledPods) {
                        assertFalse(nodesAssignedToPodsWithAffinityRequirements.contains(assignedNode));
                    } else if (podsToAssign.contains(podName) && conditionToRemainingPods) {
                        assertFalse(nodesAssignedToPodsWithoutAffinityRequirements.contains(assignedNode));
                    }
                }
            }
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static Stream testPodAffinity() {
        final String topologyKey = "kubernetes.io/hostname";
        final List<PodAffinityTerm> inTerm = List.of(term(topologyKey,
                                                       podExpr("k1", "In", "l1", "l2")));
        final List<PodAffinityTerm> existsTerm = List.of(term(topologyKey,
                                                                podExpr("k1", "Exists", "l1", "l2")));
        final List<PodAffinityTerm> notInTerm = List.of(term(topologyKey,
                                                          podExpr("k1", "NotIn", "l1", "l2")));
        final List<PodAffinityTerm> notExistsTerm = List.of(term(topologyKey,
                                                                   podExpr("k1", "DoesNotExist", "l1", "l2")));

        return Stream.of(
                // First, we test to see if all our operators work on their own

                // --------- Pod Affinity -----------
                // In
                argGen("Affinity", inTerm, map("k1", "l1"), true, false, false),
                argGen("Affinity", inTerm, map("k1", "l2"), true, false, false),
                argGen("Affinity", inTerm, map("k1", "l3"), false, false, true),
                argGen("Affinity", inTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("Affinity", inTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("Affinity", inTerm, map("k", "l", "k1", "l3"), false, false, true),

                // Exists
                argGen("Affinity", existsTerm, map("k1", "l1"), true, false, false),
                argGen("Affinity", existsTerm, map("k1", "l2"), true, false, false),
                argGen("Affinity", existsTerm, map("k1", "l3"), true, false, false),
                argGen("Affinity", existsTerm, map("k2", "l3"), false, false, true),
                argGen("Affinity", existsTerm, map("k2", "l1"), false, false, true),
                argGen("Affinity", existsTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("Affinity", existsTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("Affinity", existsTerm, map("k", "l", "k1", "l3"), true, false, false),
                argGen("Affinity", existsTerm, map("k", "l", "k2", "l1"), false, false, true),
                argGen("Affinity", existsTerm, map("k", "l", "k2", "l2"), false, false, true),
                argGen("Affinity", existsTerm, map("k", "l", "k2", "l3"), false, false, true),

                // NotIn
                argGen("Affinity", notInTerm, map("k1", "l1"), false, true, false),
                argGen("Affinity", notInTerm, map("k1", "l2"), false, true, false),
                argGen("Affinity", notInTerm, map("k1", "l3"), true, true, false),
                argGen("Affinity", notInTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("Affinity", notInTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("Affinity", notInTerm, map("k", "l", "k1", "l3"), true, true, false),

                // DoesNotExist
                argGen("Affinity", notExistsTerm, map("k1", "l1"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k1", "l2"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k1", "l3"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k", "l", "k1", "l3"), false, true, false),

                // --------- Pod Anti Affinity -----------
                // In
                argGen("AntiAffinity", inTerm, map("k1", "l1"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k1", "l2"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k1", "l3"), false, false, false),
                argGen("AntiAffinity", inTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k", "l", "k1", "l3"), false, false, false),

                // Exists
                argGen("AntiAffinity", existsTerm, map("k1", "l1"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k1", "l2"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k1", "l3"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k2", "l3"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k2", "l1"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k1", "l3"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k2", "l1"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k2", "l2"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k2", "l3"), false, false, false),

                // NotIn
                argGen("AntiAffinity", notInTerm, map("k1", "l1"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k1", "l2"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k1", "l3"), false, false, false),
                argGen("AntiAffinity", notInTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k", "l", "k1", "l3"), false, false, false),

                // DoesNotExist
                argGen("AntiAffinity", notExistsTerm, map("k1", "l1"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k1", "l2"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k1", "l3"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k", "l", "k1", "l3"), false, true, false)
        );
    }

    private static Arguments argGen(final String scenario, final List<PodAffinityTerm> terms,
                                    final Map<String, String> podLabelsInput,
                                    final boolean shouldBeAffineToLabelledPods,
                                    final boolean shouldBeAffineToRemainingPods,
                                    final boolean cannotBePlacedAnywhere) {
        final String termsString = terms.stream().map(PodAffinityTerm::getLabelSelector)
                .map(expr -> expr.getMatchExpressions().stream()
                        .map(e -> String.format("%s %s %s", e.getKey(), e.getOperator(), e.getValues()))
                        .collect(Collectors.joining(", "))
                ).collect(Collectors.joining(" or "));
        final String effect = scenario.equals("AntiAffinity") ? "anti-affine" : "affine";
        final String outcomeString =  List.of(
                shouldBeAffineToLabelledPods ? String.format("should be %s to themselves", effect) : "",
                shouldBeAffineToRemainingPods ? String.format("should be %s to remaining pods", effect) : "",
                cannotBePlacedAnywhere ? "cannot be placed anywhere" : "",
                !shouldBeAffineToLabelledPods && !shouldBeAffineToRemainingPods && !cannotBePlacedAnywhere
                     ? "can be placed anywhere" : "")
                .stream().filter(e -> e.length() > 0)
                .collect(Collectors.joining(" and "));
        final String label = String.format("%s: pods with term {%s} and labels %s, %s", scenario, termsString,
                podLabelsInput, outcomeString);
        return Arguments.of(label, scenario, terms, podLabelsInput, shouldBeAffineToLabelledPods,
                            shouldBeAffineToRemainingPods, cannotBePlacedAnywhere);
    }

    /*
     * Capacity constraints
     */
    @ParameterizedTest(name = "{0} => feasible:{6}")
    @MethodSource("spareCapacityValues")
    public void testSpareCapacity(final String displayName, final List<Integer> cpuRequests,
                                  final List<Integer> memoryRequests, final List<Integer> nodeCpuCapacities,
                                  final List<Integer> nodeMemoryCapacities,
                                  final boolean useHardConstraint, final boolean useSoftConstraint,
                                  final Predicate<List<String>> assertOn, final boolean feasible) {
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
            final Pod pod;

            final Map<String, Quantity> resourceRequests = new HashMap<>();
            resourceRequests.put("cpu", new Quantity(String.valueOf(cpuRequests.get(i))));
            resourceRequests.put("memory", new Quantity(String.valueOf(memoryRequests.get(i))));
            resourceRequests.put("pods", new Quantity("1"));
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
            final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(nodeCpuCapacities.get(i))));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(nodeMemoryCapacities.get(i))));
            nodeResourceEventHandler.onAdd(node);

            // Add one system pod per node
            final String podName = "system-pod-" + nodeName;
            final Pod pod;
            final String status = "Running";
            pod = newPod(podName, status, Collections.emptyMap(), Collections.emptyMap());
            pod.getSpec().setNodeName(nodeName);
            handler.onAdd(pod);
        }

        final List<String> policies = Policies.from(Policies.nodePredicates(),
                                                    Policies.capacityConstraint(useHardConstraint, useSoftConstraint));
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");
        if (feasible) {
            final Result<? extends Record> result = scheduler.runOneLoop();
            assertEquals(numPods, result.size());
            final List<String> nodes = result.stream()
                                            .map(e -> e.getValue("CONTROLLABLE__NODE_NAME", String.class))
                                            .collect(Collectors.toList());
            assertTrue(assertOn.test(nodes));
        } else {
            assertThrows(ModelException.class, scheduler::runOneLoop);
        }
    }


    @SuppressWarnings("UnusedMethod")
    private static Stream spareCapacityValues() {
        final Predicate<List<String>> onePodPerNode = nodes -> nodes.size() == Set.copyOf(nodes).size();
        final Predicate<List<String>> n2MustNotBeAssignedNewPods = nodes -> !nodes.contains("n2");
        final Predicate<List<String>> onlyN3MustBeAssignedNewPods = nodes -> Set.of("n3").equals(Set.copyOf(nodes));
        return Stream.of(
                Arguments.of("One pod per node",
                             List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                             List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10), true, false,
                             onePodPerNode, true),

                Arguments.of("p1 cannot be placed",
                        List.of(10, 11, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10), true, false,
                        onePodPerNode, false),

                Arguments.of("n2 does not have sufficient CPU capacity and should not host any new pods",
                        List.of(5, 5, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        List.of(10, 10, 1, 10, 10), List.of(20, 20, 20, 20, 20), true, false,
                        n2MustNotBeAssignedNewPods, true),

                Arguments.of("Only memory requests, and all pods must go to n3",
                        List.of(0, 0, 0, 0, 0), List.of(10, 10, 10, 10, 10),
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 50, 0), true, false,
                        onlyN3MustBeAssignedNewPods, true),

                Arguments.of("No resource requests: pods will be spread out by soft constraint (no hard constraint)",
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 0, 0),
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 0, 0), false, true,
                        onePodPerNode, true),

                Arguments.of("No resource requests: pods will be spread out by soft constraint (with hard constraint)",
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 0, 0),
                        List.of(0, 0, 0, 0, 0), List.of(0, 0, 0, 0, 0), true, true,
                        onePodPerNode, true)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testTaintsAndTolerationsValues")
    public void testTaintsAndTolerations(final String displayName, final List<List<Toleration>> tolerations,
                                         final List<List<Taint>> taints, final Predicate<List<String>> assertOn,
                                         final boolean feasible) {
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler handler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        final int numPods = tolerations.size();
        final int numNodes = taints.size();

        // If we only get one pod in podsToAssign, then that will be the only labelled pod. In cases of affinity
        // requirements, that means that that pod will not have any candidate nodes to be placed on.
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Pod pod = newPod(podName, "Pending", Collections.emptyMap(), Collections.emptyMap());
            if (tolerations.get(i).size() != 0) {
                pod.getSpec().setTolerations(tolerations.get(i));
            }
            handler.onAdd(pod);
        }

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            node.getSpec().setTaints(taints.get(i));
            nodeResourceEventHandler.onAdd(node);
        }
        final List<String> policies = Policies.from(Policies.nodePredicates(),
                                                    Policies.taintsAndTolerations());
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");

        if (feasible) {
            final Result<? extends Record> result = scheduler.runOneLoop();
            assertEquals(numPods, result.size());
            final List<String> nodes = result.stream()
                    .map(e -> e.getValue("CONTROLLABLE__NODE_NAME", String.class))
                    .collect(Collectors.toList());
            assertTrue(assertOn.test(nodes));
        } else {
            assertThrows(ModelException.class, scheduler::runOneLoop);
        }
    }


    @SuppressWarnings("UnusedMethod")
    private static Stream testTaintsAndTolerationsValues() {
        final Toleration tolerateK1Equals = new Toleration();
        tolerateK1Equals.setKey("k1");
        tolerateK1Equals.setOperator("Equal");
        tolerateK1Equals.setValue("v1");
        tolerateK1Equals.setEffect("NoSchedule");

        final Toleration tolerateK2Exists = new Toleration();
        tolerateK2Exists.setKey("k2");
        tolerateK2Exists.setOperator("Exists");
        tolerateK2Exists.setEffect("NoSchedule");

        final Toleration tolerateK1Exists = new Toleration();
        tolerateK1Exists.setKey("k1");
        tolerateK1Exists.setOperator("Exists");
        tolerateK1Exists.setEffect("NoSchedule");

        final Toleration tolerateK1ExistsNoExecute = new Toleration();
        tolerateK1ExistsNoExecute.setKey("k1");
        tolerateK1ExistsNoExecute.setOperator("Exists");
        tolerateK1ExistsNoExecute.setEffect("NoExecute");

        final Taint taintK1 = new Taint();
        taintK1.setKey("k1");
        taintK1.setValue("v1");
        taintK1.setEffect("NoSchedule");

        final Taint taintK1NullValue = new Taint();
        taintK1NullValue.setKey("k1");
        taintK1NullValue.setEffect("NoSchedule");

        final Taint taintK1NoExecute = new Taint();
        taintK1NoExecute.setKey("k1");
        taintK1NoExecute.setValue("v1");
        taintK1NoExecute.setEffect("NoExecute");

        final Taint taintK1V2 = new Taint();
        taintK1V2.setKey("k1");
        taintK1V2.setValue("v2");
        taintK1V2.setEffect("NoSchedule");

        final Taint taintK2V4 = new Taint();
        taintK2V4.setKey("k2");
        taintK2V4.setValue("v4");
        taintK2V4.setEffect("NoSchedule");

        final Predicate<List<String>> nodeGoesToN0 = nodes -> nodes.size() == 1 && nodes.get(0).equals("n0");
        final Predicate<List<String>> p0goesToN1andP1GoesToN0 =
                nodes -> nodes.size() == 2 && nodes.get(0).equals("n1") && nodes.get(1).equals("n0");
        final Predicate<List<String>> p0goesToN1andP1GoesToN1 =
                nodes -> nodes.size() == 2 && nodes.get(0).equals("n1") && nodes.get(1).equals("n1");
        return Stream.of(
                Arguments.of("No toleration => cannot match taint k1:v1",
                        List.of(List.of()), List.of(List.of(taintK1)),
                        null, false),

                Arguments.of("toleration k1=v1 matches taint k1:v1",
                             List.of(List.of(tolerateK1Equals)), List.of(List.of(taintK1)),
                             nodeGoesToN0, true),

                Arguments.of("toleration exists(k1) matches taint k1:v1",
                        List.of(List.of(tolerateK2Exists)), List.of(List.of(taintK2V4)),
                        nodeGoesToN0, true),

                Arguments.of("toleration k1=v1 does not match taint k1:v2",
                        List.of(List.of(tolerateK1Equals)), List.of(List.of(taintK1V2)),
                        null, false),

                Arguments.of("toleration exists(k1) does not match taint k1:v1",
                        List.of(List.of(tolerateK2Exists)), List.of(List.of(taintK1)),
                        null, false),

                Arguments.of("toleration k1=v1 does not match taints [k1:v1, k2:v4]",
                        List.of(List.of(tolerateK1Equals)), List.of(List.of(taintK1, taintK2V4)),
                        null, false),

                Arguments.of("toleration [k1=v1, exists(k2)] matches taints [k1:v1, k1:v2]",
                        List.of(List.of(tolerateK1Equals, tolerateK2Exists)), List.of(List.of(taintK1, taintK2V4)),
                        nodeGoesToN0, true),

                Arguments.of("toleration [k1=v1, exists(k2)] does not match taints [k1:v1, k1:v2, k2:v4]",
                        List.of(List.of(tolerateK1Equals, tolerateK2Exists)),
                        List.of(List.of(taintK1, taintK1V2, taintK2V4)),
                        null, false),

                Arguments.of("toleration [exists(k1), exists(k2)] matches taints [k1:v1, k1:v2, k2:v4]",
                        List.of(List.of(tolerateK1Exists, tolerateK2Exists)),
                        List.of(List.of(taintK1, taintK1V2, taintK2V4)),
                        nodeGoesToN0, true),

                Arguments.of("toleration [exists(k1), exists_noexec(k2)] does not match taints [k1:v1, k1:v2, k2:v4]",
                        List.of(List.of(tolerateK1Exists, tolerateK1ExistsNoExecute)),
                        List.of(List.of(taintK1, taintK1V2, taintK2V4)),
                        null, false),

                Arguments.of("toleration [exists(k1), exists_noexec(k2)] matches taints [k1:v1]",
                        List.of(List.of(tolerateK1Exists, tolerateK1ExistsNoExecute)), List.of(List.of(taintK1)),
                        nodeGoesToN0, true),

                Arguments.of("Multi node: p0 should go to n1 and p1 to n0.",
                        List.of(List.of(tolerateK1Equals, tolerateK2Exists),   // pod-0
                                List.of(tolerateK1ExistsNoExecute)),             // pod-1
                        List.of(List.of(taintK1NoExecute),                     // node-0
                                List.of(taintK1)),                             // node-1
                        p0goesToN1andP1GoesToN0, true),

                Arguments.of("Multi node: p0 and p1 cannot tolerate n0 so they go to n1",
                        List.of(List.of(),                                       // pod-0
                                List.of()),                                      // pod-1
                        List.of(List.of(taintK1NullValue, taintK1),            // node-0
                                List.of()),                                      // node-1
                        p0goesToN1andP1GoesToN1, true)
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

    private static NodeSelectorTerm term(final NodeSelectorRequirement... requirements) {
        return new NodeSelectorTermBuilder()
                .withMatchExpressions(requirements)
                .build();
    }

    private static NodeSelectorRequirement nodeExpr(final String key, final String op, final String... values) {
        final NodeSelectorRequirement requirement = new NodeSelectorRequirement();
        requirement.setKey(key);
        requirement.setOperator(op);
        requirement.setValues(List.of(values));
        return requirement;
    }

    private static PodAffinityTerm term(final String topologyKey, final LabelSelectorRequirement... requirements) {
        final LabelSelector labelSelector = new LabelSelectorBuilder()
                .withMatchExpressions(requirements)
                .build();
        return new PodAffinityTermBuilder()
                .withLabelSelector(labelSelector)
                .withTopologyKey(topologyKey)
                .build();
    }

    private static LabelSelectorRequirement podExpr(final String key, final String op, final String... values) {
        final LabelSelectorRequirement requirement = new LabelSelectorRequirement();
        requirement.setKey(key);
        requirement.setOperator(op);
        requirement.setValues(List.of(values));
        return requirement;
    }

    private Pod newPod(final String name) {
        return newPod(name, "Pending", Collections.emptyMap(), Collections.emptyMap());
    }

    private Pod newPod(final String podName, final String phase, final Map<String, String> selectorLabels,
                         final Map<String, String> labels) {
        final Pod pod = new Pod();
        final ObjectMeta meta = new ObjectMeta();
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp("1");
        meta.setNamespace("default");
        final PodSpec spec = new PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        spec.setNodeSelector(selectorLabels);

        final Container container = new Container();
        container.setName("pause");

        final ResourceRequirements resourceRequirements = new ResourceRequirements();
        resourceRequirements.setRequests(Collections.emptyMap());
        container.setResources(resourceRequirements);
        spec.getContainers().add(container);

        final Affinity affinity = new Affinity();
        final NodeAffinity nodeAffinity = new NodeAffinity();
        affinity.setNodeAffinity(nodeAffinity);
        spec.setAffinity(affinity);
        final PodStatus status = new PodStatus();
        status.setPhase(phase);
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }

    private Node addNode(final String nodeName, final Map<String, String> labels,
                           final List<NodeCondition> conditions) {
        final Node node = new Node();
        final NodeStatus status = new NodeStatus();
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
        final NodeSpec spec = new NodeSpec();
        spec.setUnschedulable(false);
        spec.setTaints(Collections.emptyList());
        node.setSpec(spec);
        final ObjectMeta meta = new ObjectMeta();
        meta.setName(nodeName);
        meta.setLabels(labels);
        node.setMetadata(meta);
        return node;
    }
}