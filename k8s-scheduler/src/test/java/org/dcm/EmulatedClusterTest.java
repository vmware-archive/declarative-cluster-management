/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import io.fabric8.kubernetes.client.DefaultKubernetesClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


/**
 * Tests that replay cluster traces in process without the involvement of a Kubernetes cluster.
 */
class EmulatedClusterTest {

    public void runTraceLocally() throws Exception {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();

        final ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("flowable-thread-%d").build();
        final ExecutorService service = Executors.newFixedThreadPool(10, namedThreadFactory);

        final int numNodes = 1000;

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool,
                service);

        final List<String> policies = Policies.getDefaultPolicies();
        final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, "ORTOOLS", true, 4);
        final PodResourceEventHandler handler = new PodResourceEventHandler(scheduler::handlePodEvent, service);
        scheduler.startScheduler(new EmulatedPodToNodeBinder(dbConnectionPool), 100, 50);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity("8"));
            node.getStatus().getCapacity().put("memory", new Quantity("6000"));
            node.getStatus().getCapacity().put("pods", new Quantity("110"));
            nodeResourceEventHandler.onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-" + nodeName;
            final String status = "Running";
            final Pod pod = newPod(podName, status, Collections.emptyMap(), Collections.emptyMap());
            final Map<String, Quantity> resourceRequests = new HashMap<>();
            resourceRequests.put("cpu", new Quantity("100m"));
            resourceRequests.put("memory", new Quantity("1"));
            resourceRequests.put("pods", new Quantity("1"));
            pod.getMetadata().setNamespace("kube-system");
            pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
            pod.getSpec().setNodeName(nodeName);
            handler.onAddSync(pod);
        }
        final WorkloadGeneratorIT replay = new WorkloadGeneratorIT();
        final IPodDeployer deployer = new EmulatedPodDeployer(handler, "default");
        final DefaultKubernetesClient client = new DefaultKubernetesClient();
        replay.runTrace(client, "v2-cropped.txt", deployer, "dcm-scheduler",
                100, 50, 100, 2000);
    }

    private static Node addNode(final String nodeName, final Map<String, String> labels,
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

    private static Pod newPod(final String podName, final String phase, final Map<String, String> selectorLabels,
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

    public static void main(final String[] args) throws Exception {
        final EmulatedClusterTest emulatedClusterTest = new EmulatedClusterTest();
        emulatedClusterTest.runTraceLocally();
        System.exit(0); // need this to make sure all threads correctly shut down
    }
}