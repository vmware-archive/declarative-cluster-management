/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeCondition;
import io.kubernetes.client.models.V1NodeSpec;
import io.kubernetes.client.models.V1NodeStatus;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodStatus;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        final V1Pod pod = newPod(podName);
        handler.onAdd(pod);
        assertTrue(conn.fetchExists(Tables.POD_INFO));
        assertTrue(conn.fetchExists(Tables.POD_LABELS));
        handler.onDelete(pod, true);
        assertFalse(conn.fetchExists(Tables.POD_INFO));
        assertFalse(conn.fetchExists(Tables.POD_LABELS));
    }


    /*
     * Evaluates the node predicates policy. Only one node (n5) is free of any node conditions, so all pod assignments
     * must go to that node.
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
        for (int i = 0; i < numNodes - 1; i++) {
            final V1NodeCondition badCondition = new V1NodeCondition();
            badCondition.setStatus(status);
            badCondition.setType(type);
            nodeResourceEventHandler.onAdd(addNode("n" + i, Collections.emptyMap(), List.of(badCondition)));
        }
        nodeResourceEventHandler.onAdd(addNode("n" + numNodes, Collections.emptyMap(), Collections.emptyList()));

        for (int i = 0; i < numPods; i++) {
            handler.onAdd(newPod("p" + i));
        }

        // All pod additions have completed
        final Scheduler scheduler = new Scheduler(conn, policies, "CHUFFED", true, "");
        final Result<? extends Record> results = scheduler.runOneLoop();
        assertEquals(numPods, results.size());
        results.forEach(r -> assertEquals("n5", r.get("CONTROLLABLE__NODE_NAME", String.class)));
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

    private V1Pod newPod(final String name) {
        final V1Pod pod = new V1Pod();
        final V1ObjectMeta meta = new V1ObjectMeta();
        final DateTime dateTime = new DateTime();
        meta.setName(name);
        meta.setLabels(Collections.singletonMap("key", "value"));
        meta.setCreationTimestamp(dateTime);
        meta.setNamespace("default");
        final V1PodSpec spec = new V1PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        final V1PodStatus status = new V1PodStatus();
        status.setPhase("Pending");
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }

    /*
    private V1Pod newPodWithNodeSelector(final String podName, final Map<String, String> selectorLabels) {
        final V1Pod pod = new V1Pod();
        final V1ObjectMeta meta = new V1ObjectMeta();
        final DateTime dateTime = new DateTime();
        meta.setName(podName);
        meta.setLabels(Collections.emptyMap());
        meta.setCreationTimestamp(dateTime);
        meta.setNamespace("default");
        final V1PodSpec spec = new V1PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        spec.nodeSelector(selectorLabels);
        final V1PodStatus status = new V1PodStatus();
        status.setPhase("Pending");
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }*/

    private V1Node addNode(final String nodeName, final Map<String, String> labels,
                           final List<V1NodeCondition> conditions) {
        final V1Node node = new V1Node();
        final V1NodeStatus status = new V1NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("1000"));
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