/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.util.sql.CalciteSqlStatement;
import com.vmware.ddlog.util.sql.CalciteToH2Translator;
import com.vmware.ddlog.util.sql.H2SqlStatement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.Update;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static com.vmware.dcm.DBViews.PREEMPTION_VIEW_NAME_SUFFIX;
import static com.vmware.dcm.DBViews.SCOPE_VIEW_NAME_SUFFIX;
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
    private final boolean debugMode;
    private final AtomicInteger batchId = new AtomicInteger(0);
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter solverInvocations = metrics.meter("solverInvocations");
    private final Timer solveTimes = metrics.timer(name(Scheduler.class, "solveTimes"));
    private final ThreadFactory namedThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("computation-thread-%d").build();
    private final PodEventsToDatabase podEventsToDatabase;
    private final IConnectionPool dbConnectionPool;
    private final ExecutorService scheduler = Executors.newSingleThreadExecutor(namedThreadFactory);
    private final LinkedBlockingDeque<Boolean> notificationQueue = new LinkedBlockingDeque<>();

    /**
     * Builder to instantiate a Kubernetes scheduler
     */
    public static class Builder {
        private static final int DEFAULT_SOLVER_MAX_TIME_IN_SECONDS = 1;
        private static final long DEFAULT_POD_RETRY_INTERVAL_MS = 1000;
        private static final int DEFAULT_NODE_LIMIT = 100;
        @VisibleForTesting final IConnectionPool connection;
        private List<String> initialPlacementPolicies = Policies.getInitialPlacementPolicies();
        private List<String> preemptionPolicies = Policies.getPreemptionPlacementPolicies();
        private boolean debugMode = false;
        private int numThreads = 1;
        private int solverMaxTimeInSeconds = DEFAULT_SOLVER_MAX_TIME_IN_SECONDS;
        private long retryIntervalMs = DEFAULT_POD_RETRY_INTERVAL_MS;
        private int limit = DEFAULT_NODE_LIMIT;
        @Nullable private AutoScopeViews autoScopeViews = null;
        @Nullable private Model initialPlacement = null;
        @Nullable private Model preemption = null;

        public Builder(final IConnectionPool connection) {
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
            final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                    .setNumThreads(numThreads)
                    .setPrintDiagnostics(debugMode)
                    .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
            this.initialPlacement = Model.build(connection.getDataConnectionToDb(), orToolsSolver,
                                                initialPlacementPolicies, connection.getDataConnectionToDb());
            return this;
        }

        /**
         * Configure the preemption policies for the scheduler. Each String is a self-contained SQL
         * CREATE CONSTRAINT statement.
         */
        public Builder setPreemptionPolicies(final List<String> preemptionPolicies) {
            this.preemptionPolicies = preemptionPolicies;
            final OrToolsSolver orToolsSolverPreemption = new OrToolsSolver.Builder()
                    .setNumThreads(numThreads)
                    .setPrintDiagnostics(debugMode)
                    .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
            this.preemption = Model.build(connection.getDataConnectionToDb(), orToolsSolverPreemption,
                                          preemptionPolicies, connection.getDataConnectionToDb());
            return this;
        }

        /**
         * Configure the number of worker threads used by the underlying solver instances. Defaults to 1.
         */
        public Builder setNumThreads(final int numThreads) {
            if (numThreads <= 0) {
                throw new IllegalArgumentException("numThreads has to be a positive integer");
            }
            this.numThreads = numThreads;
            return this;
        }

        /**
         * Set the solver timeout for initial placement or preemption policies. Defaults to 1 second.
         */
        public Builder setSolverMaxTimeInSeconds(final int solverMaxTimeInSeconds) {
            if (solverMaxTimeInSeconds <= 0) {
                throw new IllegalArgumentException("solverMaxTimeInSeconds has to be a positive integer");
            }
            this.solverMaxTimeInSeconds = solverMaxTimeInSeconds;
            return this;
        }

        /**
         * Configures the initial placement model to use AutoScope. Experimental. Defaults to false.
         */
        public Builder setScopedInitialPlacement(final boolean scopedInitialPlacement) {
            if (scopedInitialPlacement) {
                this.autoScopeViews = autoScopeViews(limit);
            }
            return this;
        }

        /**
         * Configures the time between scheduling attempts for pods that were previously not schedulable
         */
        public Builder setRetryIntervalMs(final long retryIntervalMs) {
            if (retryIntervalMs <= 0) {
                throw new IllegalArgumentException("retryIntervalMs has to be a positive integer");
            }
            this.retryIntervalMs = retryIntervalMs;
            return this;
        }

        /**
         * Configures the number of candidates nodes to keep from sorted tables.
         * Defaults to 20.
         * TODO: fold into setScopedInitialPlacement()
         */
        public Builder setLimit(final int limit) {
            this.limit = limit;
            return this;
        }

        public AutoScope getScope() {
            return autoScopeViews.scope();
        }

        public Scheduler build() {
            if (initialPlacement == null) {
                setInitialPlacementPolicies(initialPlacementPolicies);
            }
            if (preemption == null) {
                setPreemptionPolicies(preemptionPolicies);
            }
            return new Scheduler(connection, initialPlacement, preemption, debugMode, numThreads,
                                 solverMaxTimeInSeconds, autoScopeViews, retryIntervalMs);
        }
    }

    record AutoScopeViews(AutoScope scope, Map<String, String> augmentedViews, List<String> extraViews) { }

    static AutoScopeViews autoScopeViews(final int limit) {
        // TODO: We create a metadata connection for the base ddlog schema using H2, similar to how
        //       the DDlogJooqProvider does so. This allows us to instantiate a model with the appropriate
        //       IRContext, needed by AutoScope to do its static analysis. This is overkill: all we need
        //       is some info about base tables and column types.
        // TODO: This code is also duplicated in DDlogDBConnectionPool
        final List<String> tables = DDlogDBViews.getSchema();
        final CalciteToH2Translator translator = new CalciteToH2Translator();
        final List<H2SqlStatement> unscopedTableAndViewStatements = tables.stream()
                                                                           .filter(s -> !s.startsWith("create index"))
                                                                           .map(CalciteSqlStatement::new)
                                                                           .map(translator::toH2).toList();
        final DSLContext metadataConnection = DSL.using("jdbc:h2:mem:");
        unscopedTableAndViewStatements.forEach(e -> metadataConnection.execute(e.getStatement()));
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder().build();
        final Model initialPlacement = Model.build(metadataConnection, orToolsSolver,
                                                   Policies.getInitialPlacementPolicies(), metadataConnection);
        // Automatic scoping
        final IRContext irContext = initialPlacement.getIrContext();
        final AutoScope scope = new AutoScope(limit);
        final Map<String, String> views = scope.augmentedViews(
                Policies.getInitialPlacementPolicies(), irContext);
        // Create filtering views
        final List<String> statements = scope.getSuffixViewStatements(views, SCOPE_VIEW_NAME_SUFFIX);
        return new AutoScopeViews(scope, views, statements);
    }

    private Scheduler(final IConnectionPool dbConnectionPool, final Model initialPlacement,
              final Model preemption, final boolean debugMode, final int numThreads,
              final int solverMaxTimeInSeconds, @Nullable final AutoScopeViews autoScopeViews,
              final long retryIntervalMs) {
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
        this.initialPlacement = initialPlacement;
        if (autoScopeViews != null) {
            // Register callback
            if (dbConnectionPool instanceof DDlogDBConnectionPool) {
                ((DDlogDBConnectionPool) dbConnectionPool).getProvider().registerDeltaCallBack(
                        autoScopeViews.scope().getCallBack());
            }
            // New scoping
            this.initialPlacementFunction = (s) -> scopedFunction(autoScopeViews);
        } else {
            this.initialPlacementFunction =
                    (s) -> initialPlacement.solve(s,
                            (t) -> {
                                if (dbConnectionPool instanceof DDlogDBConnectionPool) {
                                    //return dbConnectionPool.getConnectionToDb().selectFrom(t).fetch();
                                    return ((DDlogDBConnectionPool) dbConnectionPool).getProvider()
                                                .fetchTable(t.getName());
                                } else {
                                    return dbConnectionPool.getConnectionToDb().selectFrom(t).fetch();
                                }
                            });
        }
        this.preemption = preemption;
        this.debugMode = debugMode;
        LOG.info("Initialized scheduler: {} {} {} {}", debugMode, numThreads, solverMaxTimeInSeconds,
                 autoScopeViews != null);
        this.dbConnectionPool.getConnectionToDb()
                .execute(String.format("insert into timer_t values (1, %s)",
                        System.currentTimeMillis() - 1000));
    }

    void addPodBulk(final List<Pod> pods) {
        final List<Query> queries = new ArrayList<>();
        pods.forEach(
                p -> queries.addAll(podEventsToDatabase.addPod(p))
        );
        podEventsToDatabase.bulkInsert(queries);
    }

    void handlePodEvent(final PodEvent podEvent) {
        podEventsToDatabase.handle(podEvent);
        notificationQueue.add(true);
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
        final IntSupplier numPending =
                () -> {
                    final List<Integer> res = ((DDlogDBConnectionPool) dbConnectionPool).getProvider()
                                                              .fetchTable("PODS_TO_ASSIGN_NO_LIMIT_COUNT")
                                                              .getValues(0, Integer.class);
                    return res.size() == 0 ? 0 : res.get(0);
                };
        int fetchCount = numPending.getAsInt();
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
                handleAssignment(assignedPods, binder, batch, schedulingLatency);
            }

            // If there are unsuccessful placements, trigger preemption
            if (initialPlacementResult.containsKey(UNASSIGNED)) {
                if (debugMode) {
                    DebugUtils.dbDump(dbConnectionPool.getConnectionToDb(), UUID.randomUUID());
                }
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
                    handleAssignment(assignedPodsWithPreemption, binder, batch, schedulingLatencyWithPreemption);
                }
                if (preemptionResults.containsKey(UNASSIGNED)) {
                    final Result<? extends Record> unassignedPods = preemptionResults.get(UNASSIGNED);
                    unassignedPods.forEach(e -> {
                        LOG.info("pod:{} could not be assigned a node even with preemption", e.get("POD_NAME"));
                        binder.notifyFail(e);
                    });
                    if (debugMode) {
                        DebugUtils.dbDump(dbConnectionPool.getConnectionToDb(), UUID.randomUUID());
                    }
                    requeue(unassignedPods);
                }
            }
            fetchCount = numPending.getAsInt();
        }
    }

    private Map<String, ? extends Result<? extends Record>> splitByType(final Result<? extends Record> podsToAssign) {
        return podsToAssign.intoGroups(r -> {
            final boolean hasNewAssignment = !r.get("CONTROLLABLE__NODE_NAME", String.class).equals("NULL_NODE");
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
                final String podUid = r.get("UID", String.class);
                final String newNodeName = r.get("CONTROLLABLE__NODE_NAME", String.class);
                updates.add(
                        conn.update(Tables.POD_INFO)
                                .set(DSL.field(Tables.POD_INFO.NODE_NAME.getUnqualifiedName()), newNodeName)
                                .where(DSL.field(Tables.POD_INFO.UID.getUnqualifiedName()).eq(podUid))
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

    private void requeue(final Result<? extends Record> unassignedPods) {
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Update<?>> updates = new ArrayList<>();
            unassignedPods.forEach(r -> {
                final String podUid = r.get("UID", String.class);
                final String podName = r.get("POD_NAME", String.class);
                final long requeueTime = System.currentTimeMillis();
                updates.add(
                        conn.update(Tables.POD_INFO)
                                .set(DSL.field(Tables.POD_INFO.LAST_REQUEUE.getUnqualifiedName()), requeueTime)
                                .where(DSL.field(Tables.POD_INFO.UID.getUnqualifiedName()).eq(podUid))
                );
                LOG.info("Re-queuing pod {} at time: {}", podName, requeueTime);
            });
            conn.batch(updates).execute();
        }
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

    Result<? extends Record> scopedFunction(final AutoScopeViews autoScopeViews) {
        final Timer.Context solveTimer = solveTimes.time();
        final Set<String> augViews = autoScopeViews.augmentedViews().keySet();
        final Result<? extends Record> podsToAssignUpdated = initialPlacement.solve(
                "PODS_TO_ASSIGN", (t) -> {
                    final String augView = (t.getName() + SCOPE_VIEW_NAME_SUFFIX).toUpperCase();
                    final Table<?> toFetch;
                    if (augViews.contains(augView)) {
                        toFetch = table(augView);
                    } else {
                        toFetch = t;
                    }
                    if (dbConnectionPool instanceof DDlogDBConnectionPool) {
                        final long now = System.nanoTime();
                        final DDlogJooqProvider provider = ((DDlogDBConnectionPool) dbConnectionPool).getProvider();
                        final Result<Record> augResult = provider.fetchTable(toFetch.getName());
                        if (augViews.contains(augView)) {
                            System.out.println("First fetch: " + (System.nanoTime() - now));
                            // Union with top K sort results
                            final List<Record> records = autoScopeViews.scope().getSortView();
                            System.out.println("getSortView(): " + (System.nanoTime() - now));
                            final List<Record> toAdd = new ArrayList<>();
                            for (final Record r : records) {
                                if (!augResult.contains(r)) {
                                    toAdd.add(r);
                                }
                            }
                            augResult.addAll(toAdd);
                            System.out.println("scan: " + (System.nanoTime() - now));
                            final Result<Record> origResult = provider.fetchTable(t.getName());
                            System.out.println("second fetch: " + (System.nanoTime() - now));
                            LOG.info(String.format("[Scoping Optimization]: Reducing size from %d to %d",
                                    origResult.size(), augResult.size()));
                        }
                        return augResult;

                    } else {
                        return dbConnectionPool.getConnectionToDb().selectFrom(toFetch).fetch();
                    }
                });
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
                table -> {
                    final DSLContext conn = dbConnectionPool.getConnectionToDb();
                    // Make sure we use the preemption suffix only for views, not base tables
                    if (DBViews.initialPlacementViewNames().contains(table.getName())) {
                        return conn.fetch(table((table.getName() + PREEMPTION_VIEW_NAME_SUFFIX)
                                               .toLowerCase(Locale.ROOT)));
                    }
                    return conn.fetch(table);
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
        options.addOption("t", "num-threads", true,
                "Number of threads to use for or-tools");
        options.addOption("d", "debug-mode", false,
                "Enable additional debug information");
        options.addOption("rqd", "requeue-delay", true,
                "Delay before a pod is reconsidered for scheduling");
        options.addOption("ddl", "use-ddlog", false,
                "Enable ddlog as the view maintenance engine");
        options.addOption("df", "ddlogFile", true,
                "specify which ddlog program.dl to use");
        options.addOption("ddlc", "ddlog-compile-only", false,
                "Don't run the Scheduler in ddlog mode; only compile the DDlog program");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        final boolean useDDlog = cmd.hasOption("use-ddlog");
        final boolean ddlogCompileOnly = cmd.hasOption("ddlog-compile-only");
        final String ddlogFile = cmd.getOptionValue("ddlogFile");
        final IConnectionPool conn = useDDlog ? DDlogDBConnectionPool.create(ddlogFile, true) : new DBConnectionPool();
        if (useDDlog && ddlogCompileOnly) {
            // End the test
            System.exit(0);
        }
        final Scheduler scheduler = new Scheduler.Builder(conn)
                .setDebugMode(cmd.hasOption("debug-mode"))
                .setNumThreads(Integer.parseInt(cmd.getOptionValue("num-threads")))
                .setRetryIntervalMs(Long.parseLong(cmd.getOptionValue("requeue-delay")))
                .build();
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
