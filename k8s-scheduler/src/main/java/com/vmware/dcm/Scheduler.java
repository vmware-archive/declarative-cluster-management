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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static com.vmware.dcm.DBViews.PREEMPTION_VIEW_NAME_SUFFIX;
import static org.jooq.impl.DSL.table;

/**
 * A Kubernetes scheduler that assigns pods to nodes. To use this, make sure to indicate
 * "schedulerName: dcm" in the yaml file used to create a pod.
 */
public final class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private static final String ASSIGNED = "ASSIGNED";
    private static final String PREEMPT = "PREEMPT";
    private static final String UNASSIGNED = "UNASSIGNED";
    private static final String UNCHANGED = "UNCHANGED";

    // This constant is also used in our views: see scheduler_tables.sql. Do not change.
    static final String SCHEDULER_NAME = "dcm-scheduler";
    private final Function<String, Result<? extends Record>> initialPlacementFunction;
    private final Model initialPlacement;
    private final Model preemption;
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

    /**
     * Builder to instantiate a Kubernetes scheduler
     */
    public static class Builder {
        private static final int DEFAULT_SOLVER_MAX_TIME_IN_SECONDS = 1;
        private final DBConnectionPool connection;
        private List<String> initialPlacementPolicies = Policies.getInitialPlacementPolicies();
        private List<String> preemptionPolicies = Policies.getPreemptionPlacementPolicies();
        private boolean debugMode = false;
        private int numThreads = 1;
        private boolean scopedInitialPlacement = false;
        private int solverMaxTimeInSeconds = DEFAULT_SOLVER_MAX_TIME_IN_SECONDS;

        public Builder(final DBConnectionPool connection) {
            this.connection = connection;
        }

        /**
         * Configures the underlying solver to print the generated code, its execution and solver diagnostics.
         * Defaults to false.
         */
        public Builder setDebugMode(final boolean debugMode) {
            this.debugMode = debugMode;
            return this;
        }

        /**
         * Configure the initial placement policies for the scheduler. Each String is a self-contained SQL
         * CREATE CONSTRAINT statement.
         */
        public Builder setInitialPlacementPolicies(final List<String> initialPlacementPolicies) {
            this.initialPlacementPolicies = initialPlacementPolicies;
            return this;
        }

        /**
         * Configure the preemption policies for the scheduler. Each String is a self-contained SQL
         * CREATE CONSTRAINT statement.
         */
        public Builder setPreemptionPolicies(final List<String> preemptionPolicies) {
            this.preemptionPolicies = preemptionPolicies;
            return this;
        }

        /**
         * Configure the number of worker threads used by the underlying solver instances. Defaults to 1.
         */
        public Builder setNumThreads(final int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        /**
         * Set the solver timeout for initial placement or preemption policies. Defaults to 1 second.
         */
        public Builder setSolverMaxTimeInSeconds(final int solverMaxTimeInSeconds) {
            this.solverMaxTimeInSeconds = solverMaxTimeInSeconds;
            return this;
        }

        /**
         * Configures the initial placement model to use AutoScope. Experimental. Defaults to false.
         */
        public Builder setScopedInitialPlacement(final boolean scopedInitialPlacement) {
            this.scopedInitialPlacement = scopedInitialPlacement;
            return this;
        }

        public Scheduler build() {
            return new Scheduler(connection, initialPlacementPolicies, preemptionPolicies, debugMode, numThreads,
                                 solverMaxTimeInSeconds, scopedInitialPlacement);
        }
    }

    private Scheduler(final DBConnectionPool dbConnectionPool, final List<String> initialPlacementPolicies,
              final List<String> preemptionPolicies, final boolean debugMode, final int numThreads,
              final int solverMaxTimeInSeconds, final boolean scopedInitialPlacement) {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        // This is a file generated by gradle. If building from an IDE without gradle, this file may not
        // be generated
        if (resourceAsStream != null) {
            try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                    StandardCharsets.UTF_8))) {
                final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
                LOG.info("Starting DCM Kubernetes scheduler. Build info: {}", gitProperties);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        this.dbConnectionPool = dbConnectionPool;
        this.podEventsToDatabase = new PodEventsToDatabase(dbConnectionPool);
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                .setNumThreads(numThreads)
                .setPrintDiagnostics(debugMode)
                .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
        this.initialPlacement = Model.build(dbConnectionPool.getConnectionToDb(), orToolsSolver,
                                            initialPlacementPolicies);
        final ScopedModel scopedModel = new ScopedModel(dbConnectionPool.getConnectionToDb(), initialPlacement);
        this.initialPlacementFunction = scopedInitialPlacement ? scopedModel::solve : initialPlacement::solve;
        final OrToolsSolver orToolsSolverPreemption = new OrToolsSolver.Builder()
                .setNumThreads(numThreads)
                .setPrintDiagnostics(debugMode)
                .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
        this.preemption = Model.build(dbConnectionPool.getConnectionToDb(), orToolsSolverPreemption,
                                      preemptionPolicies);
        LOG.info("Initialized scheduler: {} {} {} {} {} {}", initialPlacementPolicies, preemptionPolicies, debugMode,
                 numThreads, solverMaxTimeInSeconds, scopedInitialPlacement);
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

    void startScheduler(final IPodToNodeBinder binder) {
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
                            final UUID uuid = UUID.randomUUID();
                            LOG.error("Received Model Exception (reason: {}). Dumping DB state to /tmp/{}",
                                      e.reason(), uuid, e);
                            DebugUtils.dbDump(dbConnectionPool.getConnectionToDb(), uuid);
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
            final Map<String, ? extends Result<? extends Record>> initialPlacementResult =
                                                                 splitByType(podsToAssignUpdated);
            final long schedulingLatency = System.nanoTime() - now;
            solverInvocations.mark();

            // Handle successful placements first
            if (initialPlacementResult.containsKey(ASSIGNED)) {
                // Only consider pods that were previously unassigned.
                final Result<? extends Record> assignedPods = initialPlacementResult.get(ASSIGNED);
                fetchCount -= assignedPods.size();
                handleAssignment(assignedPods, binder, batch, schedulingLatency);
            }

            // If there are unsuccessful placements, trigger preemption
            if (initialPlacementResult.containsKey(UNASSIGNED)) {
                initialPlacementResult.get(UNASSIGNED).forEach(
                        e -> LOG.info("pod:{} could not be assigned a node in this iteration. Attempting Preemption",
                                e.get("POD_NAME"))
                );
                final Map<String, ? extends Result<? extends Record>> preemptionResults = splitByType(preempt());
                final long schedulingLatencyWithPreemption = System.nanoTime() - now;
                if (preemptionResults.containsKey(PREEMPT)) {
                    final Result<? extends Record> toPreempt = preemptionResults.get(PREEMPT);
                    toPreempt.forEach(e -> LOG.info("pod:{} will be preempted", e.get("POD_NAME")));
                    binder.unbindManyAsnc(toPreempt);
                }
                if (preemptionResults.containsKey(ASSIGNED)) {
                    final Result<? extends Record> assignedPodsWithPreemption = preemptionResults.get(ASSIGNED);
                    fetchCount -= assignedPodsWithPreemption.size();
                    handleAssignment(assignedPodsWithPreemption, binder, batch, schedulingLatencyWithPreemption);
                }
                if (preemptionResults.containsKey(UNASSIGNED)) {
                    preemptionResults.get(UNASSIGNED).forEach(
                            e -> LOG.info("pod:{} could not be assigned a node even with preemption", e.get("POD_NAME"))
                    );
                }
            }
        }
    }

    private Map<String, ? extends Result<? extends Record>> splitByType(final Result<? extends Record> podsToAssign) {
        return podsToAssign.intoGroups(r -> {
            final boolean hasNewAssignment = !r.get("CONTROLLABLE__NODE_NAME", String.class)
                    .equals("NULL_NODE");
            final boolean hadOldAssignment = r.get("NODE_NAME", String.class) != null;
            if (hasNewAssignment && !hadOldAssignment) {
                return ASSIGNED;
            } else if (!hasNewAssignment && hadOldAssignment) {
                return PREEMPT;
            } else if (!hasNewAssignment) {
                return UNASSIGNED;
            } else {
                return UNCHANGED;
            }
        });
    }

    private void handleAssignment(final Result<? extends Record> assignedPods, final IPodToNodeBinder binder,
                                  final int batch, final long totalTime) {
        // First, locally update the node_name entries for pods
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Update<?>> updates = new ArrayList<>();
            assignedPods.forEach(r -> {
                final String podName = r.get("POD_NAME", String.class);
                final String newNodeName = r.get("CONTROLLABLE__NODE_NAME", String.class);
                updates.add(
                        conn.update(Tables.POD_INFO)
                                .set(Tables.POD_INFO.NODE_NAME, newNodeName)
                                .where(Tables.POD_INFO.POD_NAME.eq(podName))
                );
                LOG.info("Scheduling decision for pod {} as part of batch {} made in time: {}",
                        podName, batch, totalTime);
            });
            conn.batch(updates).execute();
        }
        LOG.info("Done with updates");
        // Next, issue bind requests for pod -> node_name
        binder.bindManyAsnc(assignedPods);
        LOG.info("Done with bindings");
    }


    Result<? extends Record> initialPlacement() {
        final Timer.Context solveTimer = solveTimes.time();
        final Result<? extends Record> podsToAssignUpdated;
        podsToAssignUpdated = initialPlacementFunction.apply("PODS_TO_ASSIGN");
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
     * We use two sets of views to fetch data required for the DCM models. These views correspond to different kinds
     * of problems, like initial placement and preemption. See {@link DBViews}. For preemption, all view names are
     * suffixed with PREEMPTION_VIEW_NAME_SUFFIX.
     */
    Result<? extends Record> preempt() {
        final Timer.Context solveTimer = solveTimes.time();
        final Result<? extends Record> podsToAssignUpdated = preemption.solve("PODS_TO_ASSIGN",
                table -> dbConnectionPool.getConnectionToDb()
                                         .fetch(table(table.getName() + PREEMPTION_VIEW_NAME_SUFFIX)));
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
        options.addRequiredOption("t", "num-threads", true,
                "Number of threads to use for or-tools");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        final DBConnectionPool conn = new DBConnectionPool();
        final Scheduler scheduler = new Scheduler.Builder(conn)
                .setDebugMode(Boolean.parseBoolean(cmd.getOptionValue("debug-mode")))
                .setNumThreads(Integer.parseInt(cmd.getOptionValue("num-threads"))).build();
        final KubernetesClient kubernetesClient = new DefaultKubernetesClient();
        LOG.info("Running a scheduler that connects to a Kubernetes cluster on {}",
                 kubernetesClient.getConfiguration().getMasterUrl());

        final KubernetesStateSync stateSync = new KubernetesStateSync(kubernetesClient);
        stateSync.setupInformersAndPodEventStream(conn, scheduler::handlePodEvent);
        final KubernetesBinder binder = new KubernetesBinder(kubernetesClient);
        scheduler.startScheduler(binder);
        stateSync.startProcessingEvents();
        Thread.currentThread().join();
    }
}
