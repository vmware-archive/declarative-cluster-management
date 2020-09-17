/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ListenableFuture;
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class EndToEndBenchmark {

    @Param("ORTOOLS")
    static String solverToUse;

    @Param({"1", "2", "4"})
    static int numThreads;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Nullable PodResourceEventHandler handler = null;
        @Nullable EmulatedPodToNodeBinder binder = null;

        @Setup(Level.Iteration)
        public void setUp() {
            final DBConnectionPool dbConnectionPool = new DBConnectionPool();
            binder = new EmulatedPodToNodeBinder(dbConnectionPool);
            final int numNodes = 1000;

            // Add all nodes
            final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);

            final List<String> policies = Policies.getDefaultPolicies();
            final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, solverToUse, true, numThreads);
            handler = new PodResourceEventHandler(scheduler::handlePodEvent);
            scheduler.startScheduler(binder, 100, 500);
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
            meta.setResourceVersion("10");
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

    @Benchmark
    public void testSinglePodPlacement(final BenchmarkState state)
            throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            final Pod podToAdd = state.newPod("pod-" + i,
                    "Pending", Collections.emptyMap(), Collections.emptyMap());
            final ListenableFuture<Boolean> booleanListenableFuture = state.binder.waitForPodBinding("pod-" + i);
            state.handler.onAdd(podToAdd);
            booleanListenableFuture.get();
        }
    }
}
