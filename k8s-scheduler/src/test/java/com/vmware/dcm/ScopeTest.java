/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.records.PodInfoRecord;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Scope
 */
@ExtendWith({})
public class ScopeTest {
    private static final int NUM_THREADS = 1;

    /*
     * Test if Scope limits candidate nodes according to spare resources sorting
     */
    @Test
    public void testScopedSchedulerSimple() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> policies = Policies.getDefaultPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final int numNodes = 100;
        final int numPods = 60;
        conn.insertInto(Tables.BATCH_SIZE).values(numPods).execute();

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // more spare capacity as i increases
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + 1000 * i / numNodes)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
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

        final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, "ORTOOLS", true, NUM_THREADS);
        scheduler.setScopeOn();
        scheduler.scheduleAllPendingPods(new EmulatedPodToNodeBinder(dbConnectionPool));

        // Check that all pods have been scheduled to a node eligible by the scope filtering
        final Result<PodInfoRecord> fetch = conn.selectFrom(Tables.POD_INFO).fetch();
        assertEquals(numNodes + numPods, fetch.size());
        fetch.forEach(e -> assertTrue(e.getNodeName() != null
                && e.getNodeName().startsWith("n")
                && (Integer.parseInt(e.getNodeName().substring(1)) >= numNodes - numPods
                  || !e.getPodName().startsWith("p"))));
    }

    /*
     * Test if Scope filters out nodes maintaining while matching labels
     */
    @Test
    public void testScopedSchedulerAffinity() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> policies = Policies.getDefaultPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final int numNodes = 1000;
        final int numPods = 20;
        conn.insertInto(Tables.BATCH_SIZE).values(numPods).execute();

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Map<String, String> nodeLabels = new HashMap<>();
            if (i < 2) { // gpu: nodes 0..1
                nodeLabels.put("gpu", "true");
            }
            if (i > 0 && i < 16) { // ssd: nodes 1..15
                nodeLabels.put("ssd", "true");
            }
            final Node node = newNode(nodeName, nodeLabels, Collections.emptyList());
            // more spare capacity as i increases
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10 + 10 * i / numNodes)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + 1000 * i / numNodes)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            nodeResourceEventHandler.onAddSync(node);
        }

        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Map<String, String> selectorLabels = new HashMap<>();
            if (i < 8) { // pods requiring gpu: 0..7
                selectorLabels.put("gpu", "true");
            }
            if (i > 1 && i < 10) { // pods requiring ssd: 2..9
                selectorLabels.put("ssd", "true");
            }
            final Pod newPod = newPod(podName, UUID.randomUUID(), "Pending", selectorLabels, Collections.emptyMap());
            handler.onAddSync(newPod);
        }

        final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, "ORTOOLS", true, NUM_THREADS);
        scheduler.setScopeOn();
        scheduler.scheduleAllPendingPods(new EmulatedPodToNodeBinder(dbConnectionPool));

        // Check that all pods have been scheduled to a node eligible by the scope filtering
        final Result<PodInfoRecord> fetch = conn.selectFrom(Tables.POD_INFO).fetch();
        assertEquals(numPods, fetch.size());
        fetch.forEach(e -> assertTrue(e.getNodeName() != null
                && (Integer.parseInt(e.getPodName().substring(1)) >= 10
                  || Integer.parseInt(e.getNodeName().substring(1)) < 16)));
    }


    static Pod newPod(final String name) {
        return newPod(name, "Pending");
    }

    static Pod newPod(final String name, final String status) {
        return newPod(name, UUID.randomUUID(), status, Collections.emptyMap(), Collections.emptyMap());
    }

    static Pod newPod(final String podName, final UUID uid, final String phase,
                      final Map<String, String> selectorLabels, final Map<String, String> labels) {
        final Pod pod = new Pod();
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp("1");
        meta.setNamespace("default");
        meta.setResourceVersion("0");
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

    static Node newNode(final String nodeName, final Map<String, String> labels,
                        final List<NodeCondition> conditions) {
        return newNode(nodeName, UUID.randomUUID(), labels, conditions);
    }

    static Node newNode(final String nodeName, final UUID uid, final Map<String, String> labels,
                 final List<NodeCondition> conditions) {
        final Node node = new Node();
        final NodeStatus status = new NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("10", "m"));
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
        meta.setUid(uid.toString());
        meta.setName(nodeName);
        meta.setLabels(labels);
        node.setMetadata(meta);
        return node;
    }
}