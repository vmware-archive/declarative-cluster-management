/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.records.PodInfoRecord;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinity;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodAntiAffinity;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.api.model.Toleration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vmware.dcm.SchedulerTest.map;
import static com.vmware.dcm.SchedulerTest.newNode;
import static com.vmware.dcm.SchedulerTest.newPod;
import static com.vmware.dcm.SchedulerTest.podExpr;
import static com.vmware.dcm.SchedulerTest.term;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Scope
 */
@ExtendWith({})
public class ScopeTest {
    /*
     * Test if Scope only keeps the least loaded nodes when no constraints are present.
     */
    @Test
    public void testSpareCapacityFilterSimple() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final Model model = buildModel(conn);
        final ScopedModel scopedModel = new ScopedModel(conn, model);

        final int numNodes = 50;
        final int numPods = 4;

        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // nodes have more spare capacity as i increases
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10 + i)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + i)));
            node.getStatus().getCapacity().put("ephemeral-storage", new Quantity(String.valueOf(1000 + i)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100 + i)));
            nodeResourceEventHandler.onAddSync(node);
        }
        for (int i = 0; i < numPods; i++) {
            handler.onAddSync(newPod("p" + i));
        }

        final Set<String> scopedNodes = scopedModel.getScopedNodes();

        // check that only the last, least cpu-loaded nodes are included
        assertEquals(numPods, scopedNodes.size());
        final IntStream leastLoadedIds = IntStream.range(numNodes - numPods, numNodes);
        leastLoadedIds.forEach(i -> assertTrue(scopedNodes.contains("n" + i)));
    }

    /*
     * Test if Scope only keeps the least loaded nodes when there is imbalance in spare resources
     */
    @Test
    public void testSpareCapacityFilterImbalance() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final Model model = buildModel(conn);
        final ScopedModel scopedModel = new ScopedModel(conn, model);

        final int numNodes = 200;
        final int numPods = 10;
        final Set<Integer> randNodesCpu = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());
        final Set<Integer> randNodesMem = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());
        final Set<Integer> randNodesEph = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());
        final Set<Integer> randNodesPod = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());

        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // randomly selected nodes have more available resources
            if (randNodesCpu.contains(i)) {
                node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(20)));
            } else {
                node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10)));
            }
            if (randNodesMem.contains(i)) {
                node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1500)));
            } else {
                node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + i)));
            }
            if (randNodesEph.contains(i)) {
                node.getStatus().getCapacity().put("ephemeral-storage", new Quantity(String.valueOf(2000)));
            } else {
                node.getStatus().getCapacity().put("ephemeral-storage", new Quantity(String.valueOf(1000)));
            }
            if (randNodesPod.contains(i)) {
                node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(110)));
            } else {
                node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            }
            nodeResourceEventHandler.onAddSync(node);
        }
        for (int i = 0; i < numPods; i++) {
            handler.onAddSync(newPod("p" + i));
        }

        final Set<String> scopedNodes = scopedModel.getScopedNodes();

        assertTrue(numPods <= scopedNodes.size() && scopedNodes.size() < numNodes);
        // only the nodes with more available resources can be included
        scopedNodes.forEach(n -> assertTrue(randNodesCpu.contains(Integer.parseInt(n.substring(1)))
                || randNodesMem.contains(Integer.parseInt(n.substring(1)))
                || randNodesEph.contains(Integer.parseInt(n.substring(1)))
                || randNodesPod.contains(Integer.parseInt(n.substring(1)))));
    }

    /*
     * Test if scope includes nodes required by inter-pod-affinity
     */
    @Test
    public void testPodAffinityMatch() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final Model model = buildModel(conn);
        final ScopedModel scopedModel = new ScopedModel(conn, model);

        final int numNodes = 10;
        final int randNode = ThreadLocalRandom.current().nextInt(numNodes);
        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // randNode has less spare CPU than other nodes
            if (i == randNode) {
                node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(2)));
            } else {
                node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10)));
            }
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            nodeResourceEventHandler.onAddSync(node);
        }
        // Add an already running pod in randNode
        final Pod podRunning = newPod("pod-running", "Running");
        podRunning.getSpec().setNodeName("n" + randNode);
        podRunning.getMetadata().setLabels(map("k", "v"));
        handler.onAddSync(podRunning);

        // Add a pending pod affine to the running one
        final Pod pod = newPod("pod-pending");
        final PodAffinity podAffinity = new PodAffinity();
        final List<PodAffinityTerm> terms =
                podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
        terms.addAll(List.of(
                term("kubernetes.io/hostname", podExpr("k", "In", "v"))));
        pod.getSpec().getAffinity().setPodAffinity(podAffinity);
        handler.onAddSync(pod);

        final Set<String> scopedNodes = scopedModel.getScopedNodes();

        assertTrue(scopedNodes.size() < numNodes);
        // check that the loaded node containing the affine pod is included in the scope
        assertTrue(scopedNodes.contains("n" + randNode));
    }

    /*
     * Test if scope includes tainted nodes that are tolerated by pods to be assigned
     */
    @Test
    public void testPodTolerations() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final Model model = buildModel(conn);
        final ScopedModel scopedModel = new ScopedModel(conn, model);

        final Taint taintUntolerated = new Taint();
        taintUntolerated.setKey("taint");
        taintUntolerated.setValue("untolerated");
        taintUntolerated.setEffect("NoSchedule");
        final Taint taintTolerated = new Taint();
        taintTolerated.setKey("taint");
        taintTolerated.setValue("tolerated");
        taintTolerated.setEffect("NoSchedule");

        final Toleration toleration = new Toleration();
        toleration.setKey("taint");
        toleration.setOperator("Equal");
        toleration.setValue("tolerated");
        toleration.setEffect("NoSchedule");

        final int numPods = 2;
        for (int i = 0; i < numPods; i++) {
            final Pod pod = newPod("p" + i);
            pod.getSpec().setTolerations(List.of(toleration));
            handler.onAddSync(pod);
        }

        // Add tainted nodes
        final int numNodes = 10;
        final Set<Integer> randToleratedNodes = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(2).boxed().collect(Collectors.toSet());
        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            if (randToleratedNodes.contains(i)) {
                // two of the nodes are tainted as taint:tolerated
                node.getSpec().setTaints(List.of(taintTolerated));
            } else {
                // the rest are tainted as taint:untolerated
                node.getSpec().setTaints(List.of(taintUntolerated));
            }
            nodeResourceEventHandler.onAddSync(node);
        }

        final Set<String> scopedNodes = scopedModel.getScopedNodes();

        assertTrue(numPods <= scopedNodes.size() && scopedNodes.size() < numNodes);
        // check that the scope contains the tolerated nodes
        assertTrue(scopedNodes.containsAll(randToleratedNodes.stream().map(x -> "n" + x).collect(Collectors.toSet())));
    }

    @Test
    public void testEmptyPodBatch() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final List<String> policies = Policies.getInitialPlacementPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final int numNodes = 10;
        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            nodeResourceEventHandler.onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-n" + i;
            final Pod pod = newPod(podName, "Running");
            pod.getSpec().setNodeName("n" + i);
            handler.onAddSync(pod);
        }

        // Don't add any pending pods

        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                .setInitialPlacementPolicies(policies)
                .setScopedInitialPlacement(true)
                .setDebugMode(true).setLimit(numNodes).build();
        scheduler.initialPlacement();

        // finished with no errors
    }

    @Test
    public void testEmptyPodBatch() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final List<String> policies = Policies.getInitialPlacementPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final int numNodes = 10;
        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            nodeResourceEventHandler.onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-n" + i;
            final Pod pod = newPod(podName, "Running");
            pod.getSpec().setNodeName("n" + i);
            handler.onAddSync(pod);
        }

        // Don't add any pending pods

        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                .setInitialPlacementPolicies(policies)
                .setScopedInitialPlacement(true)
                .setDebugMode(true).build();
        scheduler.initialPlacement();
    }

    /*
     * E2E test with scheduler:
     * Test if Scope limits candidate nodes according to spare resources
     */
    @Test
    public void testSchedulerCapacity() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> policies = Policies.getInitialPlacementPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final int numNodes = 200;
        final int numPods = 10;

        final Set<Integer> randNodesCpu = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());
        final Set<Integer> randNodesMem = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());
        final Set<Integer> randNodesEph = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());
        final Set<Integer> randNodesPod = ThreadLocalRandom.current().ints(0, numNodes)
                .distinct().limit(numPods).boxed().collect(Collectors.toSet());

        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // randomly selected nodes have more available resources
            if (randNodesCpu.contains(i)) {
                node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(20)));
            } else {
                node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10)));
            }
            if (randNodesMem.contains(i)) {
                node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1500)));
            } else {
                node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + i)));
            }
            if (randNodesEph.contains(i)) {
                node.getStatus().getCapacity().put("ephemeral-storage", new Quantity(String.valueOf(2000)));
            } else {
                node.getStatus().getCapacity().put("ephemeral-storage", new Quantity(String.valueOf(1000)));
            }
            if (randNodesPod.contains(i)) {
                node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(110)));
            } else {
                node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            }
            nodeResourceEventHandler.onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-n" + i;
            final Pod pod = newPod(podName, "Running");
            pod.getSpec().setNodeName("n" + i);
            handler.onAddSync(pod);
        }

        for (int i = 0; i < numPods; i++) {
            handler.onAddSync(newPod("p" + i));
        }

        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                                                 .setInitialPlacementPolicies(policies)
                                                 .setScopedInitialPlacement(true)
                                                 .setDebugMode(true).setLimit(numPods).build();
        scheduler.scheduleAllPendingPods(new EmulatedPodToNodeBinder(dbConnectionPool));


        // Check that all pods have been scheduled to a node eligible by the scope filtering
        final Result<PodInfoRecord> fetch = conn.selectFrom(Tables.POD_INFO).fetch();
        assertEquals(numNodes + numPods, fetch.size());
        fetch.forEach(e -> assertTrue(e.getNodeName() != null
                && e.getNodeName().startsWith("n")
                // check that new pods have been scheduled to a scoped node
                && (randNodesCpu.contains(Integer.parseInt(e.getNodeName().substring(1)))
                || randNodesMem.contains(Integer.parseInt(e.getNodeName().substring(1)))
                || randNodesEph.contains(Integer.parseInt(e.getNodeName().substring(1)))
                || randNodesPod.contains(Integer.parseInt(e.getNodeName().substring(1)))
                || !e.getPodName().startsWith("p"))));
    }

    /*
     * E2E test with scheduler:
     * Test if Scope filters out nodes maintaining while maintaining matching labels.
     * Labeled nodes have low spare capacity.
     */
    @Test
    public void testSchedulerNodeLabels() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> policies = Policies.getInitialPlacementPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final int numNodes = 100;
        final int numPods = 20;

        final List<Integer> gpuNodesIdx = List.of(0, 1);
        final List<Integer> ssdNodesIdx = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Map<String, String> nodeLabels = new HashMap<>();
            if (gpuNodesIdx.contains(i)) {
                nodeLabels.put("gpu", "true");
            }
            if (ssdNodesIdx.contains(i)) {
                nodeLabels.put("ssd", "true");
            }
            final Node node = newNode(nodeName, nodeLabels, Collections.emptyList());

            // nodes have more spare capacity as i increases (favoring unlabeled nodes)
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10 + 10 * i / numNodes)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + (1000 * i) / numNodes)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            nodeResourceEventHandler.onAddSync(node);
        }

        final List<Integer> gpuPodsIdx = List.of(0, 1, 2, 3, 4, 5, 6, 7);
        final List<Integer> ssdPodsIdx = List.of(2, 3, 4, 5, 6, 7, 8, 9);
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Map<String, String> selectorLabels = new HashMap<>();
            if (gpuPodsIdx.contains(i)) {
                selectorLabels.put("gpu", "true");
            }
            if (ssdPodsIdx.contains(i)) {
                selectorLabels.put("ssd", "true");
            }
            final Pod newPod = newPod(podName, UUID.randomUUID(), "Pending", selectorLabels, Collections.emptyMap());
            handler.onAddSync(newPod);
        }

        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                                                .setInitialPlacementPolicies(policies)
                                                .setScopedInitialPlacement(true)
                                                .setDebugMode(true).setLimit(numPods).build();
        scheduler.scheduleAllPendingPods(new EmulatedPodToNodeBinder(dbConnectionPool));

        // Check that all pods have been scheduled to a node eligible by the scope filtering
        final Result<PodInfoRecord> fetch = conn.selectFrom(Tables.POD_INFO).fetch();
        assertEquals(numPods, fetch.size());
        fetch.forEach(e -> assertTrue(e.getNodeName() != null
                // gpuPod => gpuNode
                && (!gpuPodsIdx.contains(Integer.parseInt(e.getPodName().substring(1)))
                || gpuNodesIdx.contains(Integer.parseInt(e.getNodeName().substring(1))))
                // ssdPod => ssdNode
                && (!ssdPodsIdx.contains(Integer.parseInt(e.getPodName().substring(1)))
                || ssdNodesIdx.contains(Integer.parseInt(e.getNodeName().substring(1))))));
    }

    /*
     * E2E test with scheduler:
     * Test if Scope does not filter out nodes required by inter-pod affinity.
     *
     * Pods in group 0 are affine to each other.
     * Pods in group 1 are affine to an already running pod.
     * Pods in group 2 are anti-affine to each other and to a running pod.
     */
    @Test
    public void testSchedulerPodAffinitiesMixed() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);
        final List<String> policies = Policies.getInitialPlacementPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final String topologyKey = "kubernetes.io/hostname";

        // Three pods affine to each other
        final int group0Pods = 3;
        for (int i = 0; i < group0Pods; i++) {
            final Pod pod = newPod("pod-group0-" + i);
            pod.getMetadata().setLabels(map("group", "group0"));

            final PodAffinity podAffinity = new PodAffinity();
            final List<PodAffinityTerm> terms =
                    podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
            terms.addAll(List.of(
                    term(topologyKey, podExpr("group", "In", "group0"))));
            pod.getSpec().getAffinity().setPodAffinity(podAffinity);

            handler.onAddSync(pod);
        }

        // Pods affine to a running master pod
        final int group1Pods = 4;
        for (int i = 0; i < group1Pods; i++) {
            final Pod pod = newPod("pod-group1-" + i);
            pod.getMetadata().setLabels(map("group", "group1"));

            final PodAffinity podAffinity = new PodAffinity();
            final List<PodAffinityTerm> terms =
                    podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
            terms.addAll(List.of(
                    term(topologyKey, podExpr("master", "In", "group1-master"))));
            pod.getSpec().getAffinity().setPodAffinity(podAffinity);

            handler.onAddSync(pod);
        }

        // Pods anti-affine to each other and to master pods
        final int group2Pods = 8;
        for (int i = 0; i < group2Pods; i++) {
            final Pod pod = newPod("pod-group2-" + i);
            pod.getMetadata().setLabels(map("group", "group2"));

            final PodAntiAffinity podAntiAffinity = new PodAntiAffinity();
            final List<PodAffinityTerm> terms =
                    podAntiAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
            terms.addAll(List.of(
                    term(topologyKey, podExpr("group", "In", "group2")),
                    term(topologyKey, podExpr("master", "Exists", ""))));
            pod.getSpec().getAffinity().setPodAntiAffinity(podAntiAffinity);

            handler.onAddSync(pod);
        }

        // Add nodes
        final int numNodes = 50;
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = newNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            nodeResourceEventHandler.onAddSync(node);
        }

        // Add an already running master pod
        final int randNode = ThreadLocalRandom.current().nextInt(numNodes);
        {
            final Pod pod = newPod("pod-group1-master", "Running");
            pod.getSpec().setNodeName("n" + randNode);
            pod.getMetadata().setLabels(map("master", "group1-master"));
            handler.onAddSync(pod);
        }

        final int numPods = group0Pods + group1Pods + group2Pods;
        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                                                .setInitialPlacementPolicies(policies)
                                                .setScopedInitialPlacement(true)
                                                .setDebugMode(true).setLimit(numPods).build();
        final Result<? extends Record> results = scheduler.initialPlacement();

        // All pods have been assigned to nodes
        assertEquals(numPods, results.size());
        results.forEach(r -> assertTrue(r.get("CONTROLLABLE__NODE_NAME") != null
                && r.get("CONTROLLABLE__NODE_NAME", String.class).startsWith("n")));

        final Set<String> group0Nodes = new HashSet<>();
        final Set<String> group2Nodes = new HashSet<>();
        for (final Record row : results) {
            final String podName = row.get("POD_NAME", String.class);
            final String nodeName = row.get("CONTROLLABLE__NODE_NAME", String.class);

            if (podName.startsWith("pod-group0")) {
                group0Nodes.add(nodeName);
            }
            if (podName.startsWith("pod-group1")) {
                // group 1 pods are affine to the master pod
                assertEquals("n" + randNode, nodeName);
            }
            if (podName.startsWith("pod-group2")) {
                group2Nodes.add(nodeName);
                // group 2 pods are anti-affine to the master pod
                assertNotEquals("n" + randNode, nodeName);
            }
        }
        // group 0 pods are affine to each other, hence are scheduled in the same node
        assertEquals(1, group0Nodes.size());
        // group 2 pods are anti-affine to each other, hence are all scheduled in different nodes
        assertEquals(group2Pods, group2Nodes.size());
    }

    private Model buildModel(final DSLContext conn) {
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .build();
        return Model.build(conn, orToolsSolver, Policies.getInitialPlacementPolicies());
    }
}
