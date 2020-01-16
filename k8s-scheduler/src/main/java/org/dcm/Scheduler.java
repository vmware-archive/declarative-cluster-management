/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.davidmoten.rx2.flowable.Transformers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
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
import org.jooq.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static org.jooq.impl.DSL.using;

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

    private final DSLContext conn;
    private final AtomicInteger batchId = new AtomicInteger(0);
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter solverInvocations = metrics.meter("solverInvocations");
    private final Histogram podsPerSchedulingEvent =
            metrics.histogram(name(Scheduler.class, "pods-per-scheduling-attempt"));
    private final Timer updateDataTimes = metrics.timer(name(Scheduler.class, "updateDataTimes"));
    private final Timer solveTimes = metrics.timer(name(Scheduler.class, "solveTimes"));
    private final Object freezeUpdates = new Object();
    @Nullable private Disposable subscription;

    Scheduler(final DSLContext conn, final List<String> policies, final String solverToUse, final boolean debugMode,
              final String fznFlags) {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
            LOG.info("Starting DCM Kubernetes scheduler. Build info: {}", gitProperties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        this.conn = conn;
        this.model = createDcmModel(conn, solverToUse, policies);
        LOG.info("Initialized scheduler:: model:{}", model);
    }

    void startScheduler(final Flowable<PodEvent> eventStream, final IPodToNodeBinder binder, final int batchCount,
                        final long batchTimeMs) {
        final PodEventsToDatabase podEventsToDatabase = new PodEventsToDatabase(conn);
        subscription = eventStream
            .map(podEvent -> {
                synchronized (freezeUpdates) {
                    return podEventsToDatabase.handle(podEvent);
                }
            })
            .filter(podEvent -> podEvent.getAction().equals(PodEvent.Action.ADDED)
                    && podEvent.getPod().getStatus().getPhase().equals("Pending")
                    && podEvent.getPod().getSpec().getNodeName() == null
                    && podEvent.getPod().getSpec().getSchedulerName().equals(
                    Scheduler.SCHEDULER_NAME)
            )
            .compose(Transformers.buffer(batchCount, batchTimeMs, TimeUnit.MILLISECONDS))
            .filter(podEvents -> !podEvents.isEmpty())
            .subscribe(
                podEvents -> {
                    podsPerSchedulingEvent.update(podEvents.size());
                    LOG.info("Received the following {} events: {}", podEvents.size(), podEvents);

                    if (conn.fetchCount(Tables.PODS_TO_ASSIGN) == 0) {
                        LOG.error("Solver invoked when there were no new pods to schedule");
                        return;
                    }
                    final int batch = batchId.incrementAndGet();

                    final long now = System.nanoTime();
                    final Result<? extends Record> podsToAssignUpdated = runOneLoop();
                    final long totalTime = System.nanoTime() - now;
                    solverInvocations.mark();

                    // First, locally update the node_name entries for pods
                    podsToAssignUpdated.parallelStream().forEach(r -> {
                        final String podName = r.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                        final String nodeName = r.get(Tables.PODS_TO_ASSIGN.CONTROLLABLE__NODE_NAME);
                        if (nodeName.equals("NULL_NODE")) {
                            LOG.info("pod:{} could not be assigned a node in this iteration", podName);
                            return;
                        }

                        LOG.info("Updated POD_INFO assignment for pod:{} with node:{}", podName, nodeName);
                        conn.update(Tables.POD_INFO)
                                .set(Tables.POD_INFO.NODE_NAME, nodeName)
                                .where(Tables.POD_INFO.POD_NAME.eq(podName))
                                .execute();
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
                                if (nodeName.equals("NULL_NODE")) {
                                    LOG.info("pod:{} could not be assigned a node in this iteration", podName);
                                    return;
                                }
                                LOG.info("Attempting to bind {}:{} to {} ", namespace, podName, nodeName);
                                binder.bindOne(namespace, podName, nodeName);
                            }
                        ));
                    LOG.info("Done with bindings");
                },
                e -> {
                    LOG.error("Received exception. Dumping DB state to /tmp/", e);
                    DebugUtils.dbDump(conn);
                }
            );
    }

    /**
     * Runs one iteration of the initial placement logic for pending pods
     */
    Result<? extends Record> runOneLoop() {
        final Timer.Context updateDataTimer = updateDataTimes.time();
        synchronized (freezeUpdates) {
            model.updateData();
        }
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
    private Model createDcmModel(final DSLContext conn, final String solverToUse, final List<String> policies) {
        switch (solverToUse) {
            case "MNZ-CHUFFED":
                final File modelFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_model.mzn");
                final File dataFile = new File(MINIZINC_MODEL_PATH + "/" + "k8s_data.dzn");
                final MinizincSolver solver = new MinizincSolver(modelFile, dataFile, new Conf());
                return Model.buildModel(conn, solver, policies);
            case "ORTOOLS":
                final OrToolsSolver orToolsSolver = new OrToolsSolver();
                return Model.buildModel(conn, orToolsSolver, policies);
            default:
                throw new IllegalArgumentException(solverToUse);
        }
    }

    /**
     * Sets up a private, in-memory database.
     */
    @VisibleForTesting
    static DSLContext setupDb() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/scheduler_tables.sql");
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                                                                                    StandardCharsets.UTF_8))) {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.H2);
            using.execute("create schema curr");
            using.execute("set schema curr");

            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);
            semiColonSeparated.forEach(using::execute);
            return using;
        } catch (final SQLException | IOException e) {
            throw new RuntimeException(e);
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

        final DSLContext conn = setupDb();
        final Scheduler scheduler = new Scheduler(conn,
                Policies.getDefaultPolicies(),
                cmd.getOptionValue("solver"),
                Boolean.parseBoolean(cmd.getOptionValue("debug-mode")),
                cmd.getOptionValue("fzn-flags"));

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