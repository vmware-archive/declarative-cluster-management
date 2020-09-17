/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.records.PodInfoRecord;
import org.jooq.DSLContext;
import org.jooq.UpdateConditionStep;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class BatchingBenchmark {

    @Param({"1", "5", "10", "50"})
    static int numPods;

    @Param({"true", "false"})
    static boolean useParallelStream;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final PodEventsToDatabase podEventsToDatabase = new PodEventsToDatabase(dbConnectionPool);

        @Setup(Level.Trial)
        public void setUp() {
            for (int i = 0; i < 10000; i++) {
                final Pod pod = newPod("pod-" + i, "Pending", Collections.emptyMap(),
                                        Collections.emptyMap());
                final PodEvent event = new PodEvent(PodEvent.Action.ADDED, pod);
                podEventsToDatabase.handle(event);
            }
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

    @Benchmark
    public void testBatchUpdate(final BenchmarkState state) {
        final IntStream stream = useParallelStream ? IntStream.range(0, numPods).parallel()
                                                   : IntStream.range(0, numPods);
        final List<PodInfoRecord> records = stream
            .mapToObj(i -> {
                    try (final DSLContext conn = state.dbConnectionPool.getConnectionToDb()) {
                        final PodInfoRecord podInfoRecord = conn.selectFrom(Tables.POD_INFO)
                                .where(Tables.POD_INFO.POD_NAME.eq("pod-" + i))
                                .fetchOne();
                        podInfoRecord.setNodeName("node-1");
                        return podInfoRecord;
                    }
                }
            ).collect(Collectors.toList());
        try (final DSLContext conn = state.dbConnectionPool.getConnectionToDb()) {
            final int[] execute = conn.batchUpdate(records).execute();
            for (int i = 0; i < numPods; i++) {
                if (execute[i] == 0) {
                    throw new RuntimeException();
                }
            }
        }
    }

    @Benchmark
    public void testBatch(final BenchmarkState state) {
        final IntStream stream = useParallelStream ? IntStream.range(0, numPods).parallel()
                : IntStream.range(0, numPods);
        final List<UpdateConditionStep<PodInfoRecord>> records = stream
            .mapToObj(i -> {
                try (final DSLContext conn = state.dbConnectionPool.getConnectionToDb()) {
                        return conn.update(Tables.POD_INFO)
                                .set(Tables.POD_INFO.NODE_NAME, "node-1")
                                .where(Tables.POD_INFO.POD_NAME.eq("pod-" + i));
                    }
                }
            ).collect(Collectors.toList());
        try (final DSLContext conn = state.dbConnectionPool.getConnectionToDb()) {
            final int[] execute = conn.batch(records).execute();
            for (int i = 0; i < numPods; i++) {
                if (execute[i] == 0) {
                    throw new RuntimeException();
                }
            }
        }
    }

    @Benchmark
    public void testMultipleUpdates(final BenchmarkState state) {
        final IntStream stream = useParallelStream ? IntStream.range(0, numPods).parallel()
                                                   : IntStream.range(0, numPods);
        final int[] execute = stream
                .map(
                    i -> {
                        try (final DSLContext conn = state.dbConnectionPool.getConnectionToDb()) {
                            return conn.update(Tables.POD_INFO)
                                    .set(Tables.POD_INFO.NODE_NAME, "node-1")
                                    .where(Tables.POD_INFO.POD_NAME.eq("pod-" + i))
                                    .execute();
                        }
                    }
                ).toArray();

        for (int i = 0; i < numPods; i++) {
            if (execute[i] == 0) {
                throw new RuntimeException();
            }
        }
    }
}