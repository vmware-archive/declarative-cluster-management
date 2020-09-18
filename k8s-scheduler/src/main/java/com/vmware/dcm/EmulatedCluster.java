/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vmware.dcm.trace.TraceReplayer;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


/**
 * Used to replay traces in-process and emulate large clusters without the involvement of an actual Kubernetes cluster.
 */
class EmulatedCluster {
    private static final Logger LOG = LoggerFactory.getLogger(EmulatedCluster.class);

    public void runTraceLocally(final int numNodes, final String traceFileName, final int cpuScaleDown,
                                final int memScaleDown, final int timeScaleDown, final int startTimeCutOff,
                                final int affinityRequirementsProportion)
            throws Exception {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();

        final ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("flowable-thread-%d").build();
        final ExecutorService service = Executors.newFixedThreadPool(10, namedThreadFactory);

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool,
                service);

        final List<String> policies = Policies.getDefaultPolicies();
        final int solverMaxTimeInSeconds = numNodes >= 5000 ? 2 : 1;
        final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, "ORTOOLS", true,
                                       4, solverMaxTimeInSeconds);
        final PodResourceEventHandler handler = new PodResourceEventHandler(scheduler::handlePodEvent, service);
        scheduler.startScheduler(new EmulatedPodToNodeBinder(dbConnectionPool), 100, 50);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity("2"));
            node.getStatus().getCapacity().put("memory", new Quantity("2000"));
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
            pod.getMetadata().setUid("13131" + i);
            pod.getMetadata().setResourceVersion("1");
            pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
            pod.getSpec().setNodeName(nodeName);
            handler.onAddSync(pod);
        }
        final TraceReplayer traceReplayer = new TraceReplayer();
        final IPodDeployer deployer = new EmulatedPodDeployer(handler, "default");
        final DefaultKubernetesClient client = new DefaultKubernetesClient();
        traceReplayer.runTrace(client, traceFileName, deployer, "dcm-scheduler", cpuScaleDown,
                memScaleDown, timeScaleDown, startTimeCutOff, affinityRequirementsProportion);
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
        final EmulatedCluster emulatedCluster = new EmulatedCluster();
        final Options options = new Options();

        options.addRequiredOption("n", "numNodes", true,
                        "Number of nodes in experiment");
        options.addRequiredOption("f", "traceFile", true,
                        "Trace file to use from k8s-scheduler/src/test/resources/");
        options.addRequiredOption("c", "cpuScaleDown", true,
                        "Factor by which to scale down CPU resource demands for pods");
        options.addRequiredOption("m", "memScaleDown", true,
                        "Factor by which to scale down Memory resource demands for pods");
        options.addRequiredOption("t", "timeScaleDown", true,
                        "Factor by which to scale down arrival rate for pods");
        options.addRequiredOption("s", "startTimeCutOff", true,
                        "N, where we replay first N seconds of trace");
        options.addOption("p", "proportion", true,
                "P, from 0 to 100, indicating the proportion of pods that have affinity requirements");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);
        final int numNodes = Integer.parseInt(cmd.getOptionValue("numNodes"));
        final String traceFile = cmd.getOptionValue("traceFile");
        final int cpuScaleDown = Integer.parseInt(cmd.getOptionValue("cpuScaleDown"));
        final int memScaleDown = Integer.parseInt(cmd.getOptionValue("memScaleDown"));
        final int timeScaleDown = Integer.parseInt(cmd.getOptionValue("timeScaleDown"));
        final int startTimeCutOff = Integer.parseInt(cmd.getOptionValue("startTimeCutOff"));
        final int affinityRequirementsProportion = Integer.parseInt(cmd.hasOption("proportion") ?
                                                                    cmd.getOptionValue("proportion") : "0");
        assert affinityRequirementsProportion >= 0 && affinityRequirementsProportion <= 100;
        LOG.info("Running experiment with parameters: numNodes: {}, traceFile: {}, cpuScaleDown: {}, " +
                    "memScaleDown: {}, timeScaleDown: {}, startTimeCutOff: {}, proportion: {}",
                numNodes, traceFile, cpuScaleDown, memScaleDown,
                timeScaleDown, startTimeCutOff, affinityRequirementsProportion);
        emulatedCluster.runTraceLocally(numNodes, traceFile, cpuScaleDown, memScaleDown, timeScaleDown,
                                            startTimeCutOff, affinityRequirementsProportion);
        System.exit(0); // without this, there are non-daemon threads that prevent JVM shutdown
    }
}