/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
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
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.using;

/**
 * A Kubernetes scheduler that assigns pods to nodes. To use this, make sure to indicate
 * "schedulerName: dcm" in the yaml file used to create a pod.
 */
public final class Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    static final String SCHEDULER_NAME = "dcm-scheduler";
    private final Model model;
    @Nullable private Disposable subscription;
    private final List<Table<?>> relevantTables = Lists.newArrayList(Tables.PODS_TO_ASSIGN,
                                                                     Tables.POD_NODE_SELECTOR_MATCHES,
                                                                     Tables.NODE_INFO,
                                                                     Tables.INTER_POD_AFFINITY_MATCHES,
                                                                     Tables.SPARE_CAPACITY_PER_NODE);

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
        this.model = createDcmModel(conn, conf, policies, relevantTables);
        LOG.info("Initialized scheduler:: model:{} relevantTables:{}", model, relevantTables);
    }

    void startScheduler(final Flowable<List<PodEvent>> eventStream) {
        subscription = eventStream.subscribe(
            podEvents -> {
                LOG.info("Received the following events: {}", podEvents);
                runOneLoop();
            }
        );
    }

    Result<? extends Record> runOneLoop() {
        model.updateData();
        final Map<String, Result<? extends Record>> podsToAssignUpdated =
                model.solveModelWithoutTableUpdates(Collections.singleton("PODS_TO_ASSIGN"));
        return podsToAssignUpdated.get("PODS_TO_ASSIGN");
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
        final Flowable<List<PodEvent>> eventStream =
                stateSync.setupInformersAndPodEventStream(conn, apiServerUrl,
                                                          Integer.parseInt(cmd.getOptionValue("batch-size")),
                                                          Long.parseLong(cmd.getOptionValue("batch-interval-ms")));
        scheduler.startScheduler(eventStream);
        Thread.currentThread().join();
    }
}