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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Binding;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectReference;
import io.kubernetes.client.util.Config;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
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
    @Nullable private Disposable subscription;
    private final List<Table<?>> relevantTables = Lists.newArrayList(Tables.PODS_TO_ASSIGN,
                                                                     Tables.POD_NODE_SELECTOR_MATCHES,
                                                                     Tables.NODE_INFO,
                                                                     Tables.INTER_POD_AFFINITY_MATCHES,
                                                                     Tables.SPARE_CAPACITY_PER_NODE,
                                                                     Tables.PODS_THAT_TOLERATE_NODE_TAINTS,
                                                                     Tables.NODES_THAT_HAVE_TOLERATIONS);

    Scheduler(final DSLContext conn, final List<String> policies, final String solverToUse, final boolean debugMode,
              final String fznFlags) {
        final Conf conf = new Conf();
        conf.setProperty("solver", solverToUse);
        LOG.info("Configuring debug mode: {}", debugMode);
        if (debugMode) {
            conf.setProperty("debug_mode", "true");
        }
        conf.setProperty("fzn_flags", fznFlags);
        conf.setProperty("mnz_model_path", "/tmp/");
        this.conn = conn;
        this.model = createDcmModel(conn, conf, policies, relevantTables);
        LOG.info("Initialized scheduler:: model:{} relevantTables:{}", model, relevantTables);
    }

    void startScheduler(final Flowable<List<PodEvent>> eventStream, final CoreV1Api v1Api) {
        subscription = eventStream.subscribe(
            podEvents -> {
                podsPerSchedulingEvent.update(podEvents.size());
                LOG.info("Received the following events: {}", podEvents);

                final int batch = batchId.incrementAndGet();

                final long now = System.nanoTime();
                final Result<? extends Record> podsToAssignUpdated = runOneLoop();
                final long totalTime = System.nanoTime() - now;
                solverInvocations.mark();

                // First, locally update the node_name entries for pods
                podsToAssignUpdated.parallelStream().forEach(r -> {
                    final String podName = r.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                    final String nodeName = r.get(Tables.PODS_TO_ASSIGN.CONTROLLABLE__NODE_NAME);
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
                            try {
                                LOG.info("Attempting to bind {}:{} to {} ", namespace, podName, nodeName);
                                bindOne(namespace, podName, nodeName, v1Api);
                            } catch (final ApiException e) {
                                LOG.error("Could not bind {} to {} due to ApiException:", podName, nodeName, e);
                            }
                        }
                    ));
                LOG.info("Done with bindings");
            }
        );
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
    private Model createDcmModel(final DSLContext conn, final Conf conf, final List<String> policies,
                                 final List<Table<?>> tables) {
        final File modelFile = new File(conf.getProperty("mnz_model_path") + "/" + "k8s_model.mzn");
        final File dataFile = new File(conf.getProperty("mnz_model_path") + "/" + "k8s_data.dzn");
        return Model.buildModel(conn, tables, policies, modelFile, dataFile, conf);
    }

    /**
     * Uses the K8s API to bind a pod to a node.
     */
    private void bindOne(final String namespace, final String podName, final String nodeName, final CoreV1Api v1Api)
                         throws ApiException {
        final V1Binding body = new V1Binding();
        final V1ObjectReference target = new V1ObjectReference();
        final V1ObjectMeta meta = new V1ObjectMeta();
        target.setKind("Node");
        target.setApiVersion("v1");
        target.setName(nodeName);
        meta.setName(podName);
        body.setTarget(target);
        body.setMetadata(meta);
        v1Api.createNamespacedBinding(namespace, body, null, null, null);
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
                Charset.forName("UTF8")))) {
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
        options.addRequiredOption("a", "apiServerUrl", true,
                "URL to connect to the k8s api-server");
        options.addRequiredOption("bc", "batch-size", true,
                "Scheduler batch size count");
        options.addRequiredOption("bi", "batch-interval-ms", true,
                "Scheduler batch interval");
        options.addRequiredOption("m", "mnz-solver", true,
                "Minizinc solver to use: GECODE, CHUFFED, ORTOOLS");
        options.addOption("d", "debug-mode", true,
                "Minizinc debug mode");
        options.addOption("f", "fzn-flags", true,
                "Flatzinc flags");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);
        final String apiServerUrl = cmd.getOptionValue("apiServerUrl");
        LOG.info("Running a scheduler that connects to a Kubernetes cluster on {}", apiServerUrl);

        final DSLContext conn = setupDb();
        final Scheduler scheduler = new Scheduler(conn,
                Policies.getDefaultPolicies(),
                cmd.getOptionValue("mnz-solver"),
                Boolean.parseBoolean(cmd.getOptionValue("debug-mode")),
                cmd.getOptionValue("fzn-flags"));
        final KubernetesStateSync stateSync = new KubernetesStateSync();

        final ApiClient client = Config.fromUrl(apiServerUrl);
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
        Configuration.setDefaultApiClient(client);
        final CoreV1Api coreV1Api = new CoreV1Api();
        final io.fabric8.kubernetes.client.Config config =
                new ConfigBuilder().withMasterUrl(apiServerUrl).build();
        final DefaultKubernetesClient fabricClient = new DefaultKubernetesClient(config);

        final Flowable<List<PodEvent>> eventStream =
                stateSync.setupInformersAndPodEventStream(conn, fabricClient,
                                                          Integer.parseInt(cmd.getOptionValue("batch-size")),
                                                          Long.parseLong(cmd.getOptionValue("batch-interval-ms")));
        scheduler.startScheduler(eventStream, coreV1Api);
        Thread.currentThread().join();
    }
}