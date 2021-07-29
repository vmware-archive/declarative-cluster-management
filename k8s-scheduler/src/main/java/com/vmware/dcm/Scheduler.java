/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.k8s.generated.Tables;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * A Kubernetes scheduler that assigns pods to nodes. To use this, make sure to indicate
 * "schedulerName: dcm" in the yaml file used to create a pod.
 */
public final class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private static final int DEFAULT_SOLVER_MAX_TIME_IN_SECONDS = 1;

    // This constant is also used in our views: see scheduler_tables.sql. Do not change.
    static final String SCHEDULER_NAME = "dcm-scheduler";
    private final Model initialPlacement;
    private final ScopedModel scopedModel;
    private boolean scopeOn = false;
    private final Model preemption;
    private final DBViews views;

    private final AtomicInteger batchId = new AtomicInteger(0);
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter solverInvocations = metrics.meter("solverInvocations");
    private final Timer solveTimes = metrics.timer(name(Scheduler.class, "solveTimes"));
    private final ThreadFactory namedThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("computation-thread-%d").build();
    private final PodEventsToDatabase podEventsToDatabase;
    private final DBConnectionPool dbConnectionPool;
    private final ExecutorService scheduler = Executors.newSingleThreadExecutor(namedThreadFactory);
    private final LinkedBlockingDeque<Boolean> notificationQueue = new LinkedBlockingDeque<>();

    Scheduler(final DBConnectionPool dbConnectionPool, final String solverToUse, final boolean debugMode,
              final int numThreads) {
        this(dbConnectionPool, Policies.getInitialPlacementPolicies(), Policies.getPreemptionPlacementPolicies(),
                solverToUse, debugMode, numThreads, DEFAULT_SOLVER_MAX_TIME_IN_SECONDS);
    }

    Scheduler(final DBConnectionPool dbConnectionPool, final String solverToUse, final boolean debugMode,
              final int numThreads, final int solverMaxTimeInSeconds) {
        this(dbConnectionPool, Policies.getInitialPlacementPolicies(), Policies.getPreemptionPlacementPolicies(),
             solverToUse, debugMode, numThreads, solverMaxTimeInSeconds);
    }


    Scheduler(final DBConnectionPool dbConnectionPool, final List<String> initialPlacementPolicies,
              final String solverToUse, final boolean debugMode, final int numThreads) {
        this(dbConnectionPool, initialPlacementPolicies, Policies.getPreemptionPlacementPolicies(),
                solverToUse, debugMode, numThreads, DEFAULT_SOLVER_MAX_TIME_IN_SECONDS);
    }

    Scheduler(final DBConnectionPool dbConnectionPool, final List<String> initialPlacementPolicies,
              final List<String> preemptionPolicies, final String solverToUse,
              final boolean debugMode, final int numThreads, final int solverMaxTimeInSeconds) {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
            LOG.info("Starting DCM Kubernetes scheduler. Build info: {}", gitProperties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        this.dbConnectionPool = dbConnectionPool;
        views = new DBViews(dbConnectionPool.getConnectionToDb());
        views.initializeViews();
        this.podEventsToDatabase = new PodEventsToDatabase(dbConnectionPool);
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                .setNumThreads(numThreads)
                .setPrintDiagnostics(debugMode)
                .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
        this.initialPlacement = Model.build(dbConnectionPool.getConnectionToDb(), orToolsSolver,
                initialPlacementPolicies);
        this.scopedModel = new ScopedModel(dbConnectionPool.getConnectionToDb(), initialPlacement);
        final OrToolsSolver orToolsSolverPreemption = new OrToolsSolver.Builder()
                .setNumThreads(numThreads)
                .setPrintDiagnostics(debugMode)
                .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
        this.preemption = Model.build(dbConnectionPool.getConnectionToDb(), orToolsSolverPreemption,
                                      preemptionPolicies);
        LOG.info("Initialized scheduler:: model:{}", initialPlacement);
    }

    void handlePodEvent(final PodEvent podEvent) {
        podEventsToDatabase.handle(podEvent);
        if (podEvent.getAction().equals(PodEvent.Action.ADDED)
            && podEvent.getPod().getStatus().getPhase().equals("Pending")
            && podEvent.getPod().getSpec().getNodeName() == null
            && podEvent.getPod().getSpec().getSchedulerName().equals(
            Scheduler.SCHEDULER_NAME)) {
            notificationQueue.add(true);
        }
    }

    void handlePodEventNoNotify(final PodEvent podEvent) {
        podEventsToDatabase.handle(podEvent);
        // Skip adding pending pod in notification queue
    }

    void startScheduler(final IPodToNodeBinder binder, final int batchCount, final long batchTimeMs) {
        scheduler.execute(
                () -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            notificationQueue.take();
                            LOG.info("Attempting schedule");
                            scheduleAllPendingPods(binder);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (final SolverException e) {
                            LOG.error("Received Model Exception (reason: {}). Dumping DB state to /tmp/",
                                      e.reason(), e);
                            DebugUtils.dbDump(dbConnectionPool.getConnectionToDb());
                        }
                    }
                }
        );
    }

    void scheduleAllPendingPods(final IPodToNodeBinder binder) {
        int fetchCount = dbConnectionPool.getConnectionToDb().fetchCount(table("PODS_TO_ASSIGN_NO_LIMIT"));
        while (fetchCount > 0) {
            LOG.info("Fetchcount is {}", fetchCount);
            final int batch = batchId.incrementAndGet();

            final long now = System.nanoTime();
            final Result<? extends Record> podsToAssignUpdated = initialPlacement();
            final Map<Boolean, ? extends Result<? extends Record>> byType = podsToAssignUpdated
                                        .intoGroups(r -> !r.get(field("CONTROLLABLE__NODE_NAME")).equals("NULL_NODE"));
            final long totalTime = System.nanoTime() - now;
            solverInvocations.mark();

            fetchCount -= podsToAssignUpdated.size();

            // Handle successful placements first
            if (byType.containsKey(true)) {
                // First, locally update the node_name entries for pods
                try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                    final List<Update<?>> updates = new ArrayList<>();
                    byType.get(true).forEach(r -> {
                        final String podName = r.get("POD_NAME", String.class);
                        final String nodeName = r.get("CONTROLLABLE__NODE_NAME", String.class);
                        updates.add(
                                conn.update(Tables.POD_INFO)
                                        .set(Tables.POD_INFO.NODE_NAME, nodeName)
                                        .where(Tables.POD_INFO.POD_NAME.eq(podName))
                        );
                        LOG.info("Scheduling decision for pod {} as part of batch {} made in time: {}",
                                podName, batch, totalTime);
                    });
                    conn.batch(updates).execute();
                }
                LOG.info("Done with updates");
                // Next, issue bind requests for pod -> node_name
                binder.bindManyAsnc(byType.get(true));
                LOG.info("Done with bindings");
            }

            // Initiate preemption for assignments that failed
            if (byType.containsKey(false)) {
                byType.get(false).forEach(
                        e -> LOG.info("pod:{} could not be assigned a node in this iteration", e.get("POD_NAME"))
                );
            }
        }
    }

    Result<? extends Record> initialPlacement() {
        final Timer.Context solveTimer = solveTimes.time();
        final Result<? extends Record> podsToAssignUpdated;
        if (scopeOn) {
            podsToAssignUpdated = scopedModel.solve("PODS_TO_ASSIGN");
        } else {
            podsToAssignUpdated = initialPlacement.solve("PODS_TO_ASSIGN");
        }
        solveTimer.stop();
        return podsToAssignUpdated;
    }

    Result<? extends Record> initialPlacement(final Function<Table<?>, Result<? extends Record>> fetcher) {
        final Timer.Context solveTimer = solveTimes.time();
        final Result<? extends Record> podsToAssignUpdated = initialPlacement.solve("PODS_TO_ASSIGN", fetcher);
        solveTimer.stop();
        return podsToAssignUpdated;
    }

    /**
     * Sets the usage of ScopedModel.
     * TODO: add to constructor
     */
    public void setScopeOn() {
        scopeOn = true;
    }

    Result<? extends Record> preempt() {
        final Timer.Context solveTimer = solveTimes.time();
        final Result<? extends Record> podsToAssignUpdated = preemption.solve("PODS_TO_ASSIGN",
                table -> {
            final String tableNameTweak = table.getName().equalsIgnoreCase("PODS_TO_ASSIGN") ||
                                          table.getName().equalsIgnoreCase("ASSIGNED_PODS") ?
                                            "_PREEMPT" : "";
                    // We control the set of pods that appear in the fixed/unfixed sets here.
                    return dbConnectionPool.getConnectionToDb()
                            .fetch(views.preemption.getQuery(table.getName() + tableNameTweak));
                });
        solveTimer.stop();
        return podsToAssignUpdated;
    }

    void shutdown() throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(100, TimeUnit.SECONDS);
    }

    public static void main(final String[] args) throws InterruptedException, ParseException {
        final Options options = new Options();

        options.addRequiredOption("bc", "batch-size", true,
                "Scheduler batch size count");
        options.addRequiredOption("bi", "batch-interval-ms", true,
                "Scheduler batch interval");
        options.addRequiredOption("m", "solver", true,
                "Solver to use: MNZ-CHUFFED, ORTOOLS");
        options.addRequiredOption("t", "num-threads", true,
                "Number of threads to use for or-tools");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        final DBConnectionPool conn = new DBConnectionPool();
        final Scheduler scheduler = new Scheduler(conn, cmd.getOptionValue("solver"),
                                                  Boolean.parseBoolean(cmd.getOptionValue("debug-mode")),
                                                  Integer.parseInt(cmd.getOptionValue("num-threads")));
        final KubernetesClient kubernetesClient = new DefaultKubernetesClient();
        LOG.info("Running a scheduler that connects to a Kubernetes cluster on {}",
                 kubernetesClient.getConfiguration().getMasterUrl());

        final KubernetesStateSync stateSync = new KubernetesStateSync(kubernetesClient);
        stateSync.setupInformersAndPodEventStream(conn, scheduler::handlePodEvent);
        final KubernetesBinder binder = new KubernetesBinder(kubernetesClient);
        scheduler.startScheduler(binder, Integer.parseInt(cmd.getOptionValue("batch-size")),
                                 Long.parseLong(cmd.getOptionValue("batch-interval-ms")));
        stateSync.startProcessingEvents();
        Thread.currentThread().join();
    }
}
