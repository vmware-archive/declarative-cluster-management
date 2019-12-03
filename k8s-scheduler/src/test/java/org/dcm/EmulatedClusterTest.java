/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

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
import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class EmulatedClusterTest extends ITBase {

    @Test
    public void runTraceLocally() throws Exception {
        final DSLContext conn = Scheduler.setupDb();
        final PublishProcessor<PodEvent> emitter = PublishProcessor.create();
        final PodResourceEventHandler podResourceEventHandler = new PodResourceEventHandler(conn, emitter);
        emitter.subscribe();

        final int numNodes = 50;

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(conn);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            nodeResourceEventHandler.onAdd(node);

            // Add one system pod per node
            final String podName = "system-pod-" + nodeName;
            final Pod pod;
            final String status = "Running";
            pod = newPod(podName, status, Collections.emptyMap(), Collections.emptyMap());
            pod.getSpec().setNodeName(nodeName);
            podResourceEventHandler.onAdd(pod);
        }
        final List<String> policies = Policies.getDefaultPolicies();
        final Scheduler scheduler = new Scheduler(conn, policies, "ORTOOLS", true, "");
        final Flowable<List<PodEvent>> flowable = emitter
                .filter(podEvent -> podEvent.getAction().equals(PodEvent.Action.ADDED)
                        && podEvent.getPod().getStatus().getPhase().equals("Pending")
                        && podEvent.getPod().getSpec().getNodeName() == null
                        && podEvent.getPod().getSpec().getSchedulerName().equals(
                        Scheduler.SCHEDULER_NAME)
                )
                .buffer(100, TimeUnit.MILLISECONDS, 100)
                .filter(podEvents -> !podEvents.isEmpty());

        scheduler.startScheduler(flowable, new EmulatedBinder(conn));

        final WorkloadGeneratorIT workloadGeneratorIT = new WorkloadGeneratorIT();
        final IDeployer deployer = new EmulatedDeployer(podResourceEventHandler, "default");
        workloadGeneratorIT.runTrace("v1-cropped.txt", deployer, "dcm-scheduler", 20, 50, 1000);
    }

    private Node addNode(final String nodeName, final Map<String, String> labels,
                         final List<NodeCondition> conditions) {
        final Node node = new Node();
        final NodeStatus status = new NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("10000"));
        quantityMap.put("memory", new Quantity("10000"));
        quantityMap.put("ephemeral-storage", new Quantity("10000"));
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
}
