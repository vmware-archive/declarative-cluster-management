/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.davidmoten.rx2.flowable.Transformers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dcm.backend.MinizincSolver;
import org.dcm.backend.OrToolsSolver;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A Kubernetes scheduler that assigns pods to nodes. To use this, make sure to indicate
 * "schedulerName: dcm" in the yaml file used to create a pod.
 */
public final class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private static final String MINIZINC_MODEL_PATH = "/tmp";

    // This constant is also used in our views: see scheduler_tables.sql. Do not change.
    static final String SCHEDULER_NAME = "dcm-scheduler";
    private final Model model;

    private final AtomicInteger batchId = new AtomicInteger(0);
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter solverInvocations = metrics.meter("solverInvocations");
    private final Histogram podsPerSchedulingEvent =
            metrics.histogram(name(Scheduler.class, "pods-per-scheduling-attempt"));
    private final Timer updateDataTimes = metrics.timer(name(Scheduler.class, "updateDataTimes"));
    private final Timer solveTimes = metrics.timer(name(Scheduler.class, "solveTimes"));
    @Nullable private Disposable subscription;
    private final ThreadFactory namedThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("computation-thread-%d").build();
    private final PodEventsToDatabase podEventsToDatabase;
    private final DBConnectionPool dbConnectionPool;

    Scheduler(final DBConnectionPool dbConnectionPool, final List<String> policies, final String solverToUse,
              final boolean debugMode, final int numThreads) {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
            LOG.info("Starting DCM Kubernetes scheduler. Build info: {}", gitProperties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        this.dbConnectionPool = dbConnectionPool;
        this.podEventsToDatabase = new PodEventsToDatabase(dbConnectionPool);
        this.model = createDcmModel(dbConnectionPool.getConnectionToDb(), solverToUse, policies, numThreads);
        LOG.info("Initialized scheduler:: model:{}", model);
    }

    void startScheduler(final Flowable<PodEvent> eventStream, final IPodToNodeBinder binder, final int batchCount,
                        final long batchTimeMs) {
        subscription = eventStream
            .map(podEventsToDatabase::handle)
            .filter(podEvent -> podEvent.getAction().equals(PodEvent.Action.ADDED)
                    && podEvent.getPod().getStatus().getPhase().equals("Pending")
                    && podEvent.getPod().getSpec().getNodeName() == null
                    && podEvent.getPod().getSpec().getSchedulerName().equals(
                    Scheduler.SCHEDULER_NAME)
            )
            .compose(Transformers.buffer(batchCount, batchTimeMs, TimeUnit.MILLISECONDS))
//            .buffer(batchTimeMs, TimeUnit.MILLISECONDS, batchCount)
            .filter(podEvents -> !podEvents.isEmpty())
            .observeOn(Schedulers.from(Executors.newSingleThreadExecutor(namedThreadFactory)))
            .subscribe(
                podEvents -> {
                    podsPerSchedulingEvent.update(podEvents.size());
                    LOG.info("Received the following {} events: {}", podEvents.size(), podEvents);
                    scheduleAllPendingPods(binder);
                },
                e -> {
                    LOG.error("Received exception. Dumping DB state to /tmp/", e);
                    DebugUtils.dbDump(dbConnectionPool.getConnectionToDb());
                }
            );
    }

    void scheduleAllPendingPods(final IPodToNodeBinder binder) {
        int fetchCount = dbConnectionPool.getConnectionToDb().fetchCount(Tables.PODS_TO_ASSIGN_NO_LIMIT);
        while (fetchCount != 0) {
            LOG.info("Fetchcount is {}", fetchCount);
            final int batch = batchId.incrementAndGet();

            final long now = System.nanoTime();
            final Result<? extends Record> podsToAssignUpdated = runOneLoop();
            final long totalTime = System.nanoTime() - now;
            solverInvocations.mark();

            fetchCount -= podsToAssignUpdated.size();

            // First, locally update the node_name entries for pods
            podsToAssignUpdated.parallelStream().forEach(r -> {
                final String podName = r.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                final String nodeName = r.get(Tables.PODS_TO_ASSIGN.CONTROLLABLE__NODE_NAME);
                try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                    conn.update(Tables.POD_INFO)
                            .set(Tables.POD_INFO.NODE_NAME, nodeName)
                            .where(Tables.POD_INFO.POD_NAME.eq(podName))
                            .execute();
                    podEventsToDatabase.reflectPodRequestsInNodeTable(nodeName,
                            r.get(Tables.PODS_TO_ASSIGN.CPU_REQUEST),
                            r.get(Tables.PODS_TO_ASSIGN.MEMORY_REQUEST),
                            r.get(Tables.PODS_TO_ASSIGN.EPHEMERAL_STORAGE_REQUEST),
                            PodEvent.Action.UPDATED);
                }
                LOG.info("Scheduling decision for pod {} as part of batch {} made in time: {}",
                         podName, batch, totalTime);
            });
            LOG.info("Done with updates");
            // Next, issue bind requests for pod -> node_name
            podsToAssignUpdated
                    .forEach((record) -> ForkJoinPool.commonPool().execute(
                            () -> {
                                final String podName = record.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                                final String namespace = record.get(Tables.PODS_TO_ASSIGN.NAMESPACE);
                                final String nodeName = record.get(Tables.PODS_TO_ASSIGN.CONTROLLABLE__NODE_NAME);
                                LOG.info("Attempting to bind {}:{} to {} ", namespace, podName, nodeName);
                                binder.bindOne(namespace, podName, nodeName);
                            }
                    ));
            LOG.info("Done with bindings");
        }
    }

    Result<? extends Record> runOneLoop() {
        final Timer.Context updateDataTimer = updateDataTimes.time();
        model.updateData();
        updateDataTimer.stop();
        final Timer.Context solveTimer = solveTimes.time();
        final Result<? extends Record> podsToAssignUpdated =
                model.solveModelWithoutTableUpdates(Collections.singleton("PODS_TO_ASSIGN"))
                     .get("PODS_TO_ASSIGN");
        solveTimer.stop();
        return podsToAssignUpdated;
    }

    /**
     * Instantiates a DCM model based on the configured policies.
     */
    private Model createDcmModel(final DSLContext conn, final String solverToUse, final List<String> policies,
                                 final int numThreads) {
        switch (solverToUse) {
            case "MNZ-CHUFFED":
                final File modelFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_model.mzn");
                final File dataFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_data.dzn");
                final MinizincSolver solver = new MinizincSolver(modelFile, dataFile, new Conf());
                return Model.buildModel(conn, solver, policies);
            case "ORTOOLS":
                final OrToolsSolver orToolsSolver = new OrToolsSolver(numThreads);
                return Model.buildModel(conn, orToolsSolver, policies);
            default:
                throw new IllegalArgumentException(solverToUse);
        }
    }

    void shutdown() {
        assert subscription != null;
        subscription.dispose();
    }

    public static void main(final String[] args) throws InterruptedException, ParseException {
        final Options options = new Options();

        options.addRequiredOption("bc", "batch-size", true,
                "Scheduler batch size count");
        options.addRequiredOption("bi", "batch-interval-ms", true,
                "Scheduler batch interval");
        options.addRequiredOption("m", "solver", true,
                "Solver to use: MNZ-CHUFFED, ORTOOLS");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        final DBConnectionPool conn = new DBConnectionPool();
        final Scheduler scheduler = new Scheduler(conn,
                Policies.getDefaultPolicies(),
                cmd.getOptionValue("solver"),
                Boolean.parseBoolean(cmd.getOptionValue("debug-mode")),
                Integer.parseInt(cmd.getOptionValue("numThreads")));

        final KubernetesClient kubernetesClient = new DefaultKubernetesClient();
        LOG.info("Running a scheduler that connects to a Kubernetes cluster on {}",
                 kubernetesClient.getConfiguration().getMasterUrl());

        final KubernetesStateSync stateSync = new KubernetesStateSync(kubernetesClient);
        final Flowable<PodEvent> eventStream = stateSync.setupInformersAndPodEventStream(conn);
        final KubernetesBinder binder = new KubernetesBinder(kubernetesClient);
        scheduler.startScheduler(eventStream, binder,
                                 Integer.parseInt(cmd.getOptionValue("batch-size")),
                                 Long.parseLong(cmd.getOptionValue("batch-interval-ms")));
        stateSync.startProcessingEvents();
        Thread.currentThread().join();
    }
}