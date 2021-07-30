/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

/*
 * ScaleNodeBenchmark
 * Measure the solver execution time while system nodes and load varies
 */

package com.vmware.dcm;

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
import org.jooq.Record;
import org.jooq.Result;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 16, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime, Mode.SingleShotTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ScaleNodeBenchmark {

    @Param({"0", "50", "70", "80"})
    static int systemMinLoad;
    @Param({"10", "50"})
    static int newPods;
    @Param({"100", "300", "1000", "3000", "10000", "30000"})
    static int numNodes;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Nullable Scheduler scheduler = null;
        @Nullable PodResourceEventHandler handler = null;
        @Nullable HashSet<Pod> addedPods = null;

        /**
         * Initialize scheduler and add #numNodes loaded nodes in the system.
         * Each is loaded at U% capacity, where U is uniformly distributed over [systemMinLoad, 100).
         * numNodes and systemMinLoad are benchmark variables.
         */
        @Setup(Level.Trial)
        public void setUp() {
            System.out.println("Running per Trial Setup...");

            final DBConnectionPool dbConnectionPool = new DBConnectionPool();
            final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
            final boolean debugMode = true;
            final int numThreads = 1;
            scheduler = new Scheduler(dbConnectionPool, debugMode, numThreads);
            handler = new PodResourceEventHandler(scheduler::handlePodEventNoNotify);
            addedPods = new HashSet<Pod>();

            for (int i = 0; i < numNodes; i++) {
                // Add node.
                final String nodeName = "n" + i;
                final Node node = addNode(nodeName, Collections.emptyMap(), Collections.emptyList());

                node.getStatus().getCapacity().put("cpu", new Quantity("100"));
                node.getStatus().getCapacity().put("memory", new Quantity("100"));
                node.getStatus().getCapacity().put("pods", new Quantity("100"));
                nodeResourceEventHandler.onAddSync(node);

                // Add a resource consuming pod.
                final String podName = "system-pod-" + nodeName;
                final String status = "Running";
                final Pod pod = newPod(podName, status, Collections.emptyMap(), Collections.emptyMap());

                final int systemLoad = ThreadLocalRandom.current().nextInt(systemMinLoad, 100);
                final Map<String, Quantity> resourceRequests = new HashMap<>();
                resourceRequests.put("cpu", new Quantity(systemLoad + ""));
                resourceRequests.put("memory", new Quantity(systemLoad + ""));
                resourceRequests.put("pods", new Quantity("1"));
                pod.getMetadata().setNamespace("kube-system");
                pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
                pod.getSpec().setNodeName(nodeName);
                handler.onAddSync(pod);
            }
        }

        /**
         * Terminate scheduler and pod event handler.
         */
        @TearDown(Level.Trial)
        public void tearDown() throws InterruptedException {
            System.out.println("Running per Trial TearDown...");

            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (handler != null) {
                handler.shutdown();
            }
        }

        /**
         * Add pending pods in the system without scheduling them.
         * Each pod requests R% of a node's resources, where R is uniformly distributed over [5, 15].
         * To be able to schedule the pod, a node should have at least R% spare capacity.
         */
        @Setup(Level.Iteration)
        public void addPods() {
            System.out.println("Running per Iteration Setup...");

            for (int i = 0; i < newPods; i++) {
                // Add pending pods to be scheduled.
                final String podName = "pod-" + i;
                final String status = "Pending";
                final Pod podToAdd = newPod(podName, status, Collections.emptyMap(), Collections.emptyMap());

                final int minPodReq = 5;
                final int maxPodReq = 15;
                final int podReq = ThreadLocalRandom.current().nextInt(minPodReq, maxPodReq);

                final Map<String, Quantity> resourceRequests = new HashMap<>();
                resourceRequests.put("cpu", new Quantity(podReq + ""));
                resourceRequests.put("memory", new Quantity(podReq + ""));
                resourceRequests.put("pods", new Quantity("1"));
                podToAdd.getMetadata().setNamespace("kube-system");
                podToAdd.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
                handler.onAddSync(podToAdd);

                // Keep track of added pods to remove them after running the solver.
                addedPods.add(podToAdd);
            }
        }

        /**
         * Prepare for next iteration by removing previous pods.
         */
        @TearDown(Level.Iteration)
        public void removePods() {
            System.out.println("Running per Iteration TearDown...");

            // Remove previous pods
            for (final Pod podToRemove : addedPods) {
                handler.onDeleteSync(podToRemove, true);
            }
            addedPods.clear();
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
            meta.setUid(UUID.randomUUID().toString());
            meta.setName(nodeName);
            meta.setLabels(labels);
            node.setMetadata(meta);
            return node;
        }

        private Pod newPod(final String podName, final String phase, final Map<String, String> selectorLabels,
                           final Map<String, String> labels) {
            final Pod pod = new Pod();
            final ObjectMeta meta = new ObjectMeta();
            meta.setUid(UUID.randomUUID().toString());
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

    /**
     * Benchmark loop to measure the solver's execution time.
     * @param state The benchmark state containing the scheduler
     */
    @Benchmark
    public void testSinglePodPlacement(final BenchmarkState state) {
        System.out.println("Running Benchmark...");

        // Scheduler's runOneLoop calls solver's updateData and solve.
        final Result<? extends Record> solverOutput = state.scheduler.initialPlacement();
        if (solverOutput.size() != newPods) {
            throw new SolverException("Could not execute solver with all " + newPods + " pods.");
        }
    }
}
