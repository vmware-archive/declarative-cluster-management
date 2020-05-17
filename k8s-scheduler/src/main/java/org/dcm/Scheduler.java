/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
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
import org.jooq.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
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
    private static final int DEFAULT_SOLVER_MAX_TIME_IN_SECONDS = 1;

    // This constant is also used in our views: see scheduler_tables.sql. Do not change.
    static final String SCHEDULER_NAME = "dcm-scheduler";
    private final Model model;

    private final AtomicInteger batchId = new AtomicInteger(0);
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter solverInvocations = metrics.meter("solverInvocations");
    private final Timer updateDataTimes = metrics.timer(name(Scheduler.class, "updateDataTimes"));
    private final Timer solveTimes = metrics.timer(name(Scheduler.class, "solveTimes"));
    private final ThreadFactory namedThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("computation-thread-%d").build();
    private final PodEventsToDatabase podEventsToDatabase;
    private final DBConnectionPool dbConnectionPool;
    private final ExecutorService scheduler = Executors.newSingleThreadExecutor(namedThreadFactory);
    private final LinkedBlockingDeque<Boolean> notificationQueue = new LinkedBlockingDeque<>();

    Scheduler(final DBConnectionPool dbConnectionPool, final List<String> policies, final String solverToUse,
              final boolean debugMode, final int numThreads) {
        this(dbConnectionPool, policies, solverToUse, debugMode, numThreads, DEFAULT_SOLVER_MAX_TIME_IN_SECONDS);
    }

    Scheduler(final DBConnectionPool dbConnectionPool, final List<String> policies, final String solverToUse,
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
        this.podEventsToDatabase = new PodEventsToDatabase(dbConnectionPool);
        this.model = createDcmModel(dbConnectionPool.getConnectionToDb(), solverToUse, policies, numThreads,
                                    solverMaxTimeInSeconds);
        LOG.info("Initialized scheduler:: model:{}", model);
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
                        } catch (final ModelException e) {
                            LOG.error("Received Model Exception. Dumping DB state to /tmp/", e);
                            DebugUtils.dbDump(dbConnectionPool.getConnectionToDb());
                        }
                    }
                }
        );
    }

    @SuppressWarnings("unchecked")
    void scheduleAllPendingPods(final IPodToNodeBinder binder) {
        int fetchCount = dbConnectionPool.getConnectionToDb().fetchCount(Tables.PODS_TO_ASSIGN_NO_LIMIT);
        while (fetchCount > 0) {
            LOG.info("Fetchcount is {}", fetchCount);
            final int batch = batchId.incrementAndGet();

            final long now = System.nanoTime();
            final Result<? extends Record> podsToAssignUpdated = runOneLoop();
            final long totalTime = System.nanoTime() - now;
            solverInvocations.mark();

            fetchCount -= podsToAssignUpdated.size();

            // First, locally update the node_name entries for pods
            try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                final List<Update<?>> updates = new ArrayList<>();
                podsToAssignUpdated.forEach(r -> {
                    final String podName = r.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                    final String nodeName = r.get(Tables.PODS_TO_ASSIGN.CONTROLLABLE__NODE_NAME);
                    updates.add(
                        conn.update(Tables.POD_INFO)
                                .set(Tables.POD_INFO.NODE_NAME, nodeName)
                                .where(Tables.POD_INFO.POD_NAME.eq(podName))
                    );
                    LOG.info("Scheduling decision for pod {} as part of batch {} made in time: {}",
                             podName, batch, totalTime);
                });
                final int[] execute = conn.batch(updates).execute();
                System.out.println(Arrays.toString(execute));
            }
            LOG.info("Done with updates");
            // Next, issue bind requests for pod -> node_name
            binder.bindManyAsnc(podsToAssignUpdated);
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
                                 final int numThreads, final int solverMaxTimeInSeconds) {
        switch (solverToUse) {
            case "MNZ-CHUFFED":
                final File modelFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_model.mzn");
                final File dataFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_data.dzn");
                final MinizincSolver solver = new MinizincSolver(modelFile, dataFile, new Conf());
                return Model.buildModel(conn, solver, policies);
            case "ORTOOLS":
                final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                                                     .setNumThreads(numThreads)
                                                     .setMaxTimeInSeconds(solverMaxTimeInSeconds).build();
                return Model.buildModel(conn, orToolsSolver, policies);
            default:
                throw new IllegalArgumentException(solverToUse);
        }
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
        final Scheduler scheduler = new Scheduler(conn,
                Policies.getDefaultPolicies(),
                cmd.getOptionValue("solver"),
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