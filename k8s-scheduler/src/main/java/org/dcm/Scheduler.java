/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
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
    private final DSLContext conn;
    private final String k8sApiUrl;
    private final long batchTimeInMs;
    private final int batchCount;
    private final Model model;
    private final DataPuller puller;
    private final List<Table<?>> relevantTables = Lists.newArrayList(Tables.POD_INFO,
                                                                    Tables.PODS_TO_ASSIGN,
                                                                    Tables.NODE_INFO,
                                                                    Tables.NODE_LABELS,
                                                                    Tables.POD_PORTS_REQUEST,
                                                                    Tables.CANDIDATE_NODES_FOR_PODS,
                                                                    Tables.POD_AFFINITY_MATCH_EXPRESSIONS,
                                                                    Tables.CANDIDATE_NODES_FOR_PODS_POD_AFFINITY,
                                                                    Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS,
                                                                    Tables.BLACKLIST_NODES_FOR_PODS_POD_ANTI_AFFINITY,
                                                                    Tables.PODS_TO_ASSIGN_WITH_POD_AFFINITY_LABELS,
                                                                    Tables.PODS_TO_ASSIGN_WITH_LABELS,
                                                                    Tables.PODS_TO_ASSIGN_WITH_POD_ANTI_AFFINITY_LABELS,
                                                                    Tables.SPARE_CAPACITY_PER_NODE,
                                                                    Tables.PENDING_SERVICES_WITH_AFFINITY_LABELS,
                                                                    Tables.SPARE_PODS_PER_NODE_PER_GROUP);

    Scheduler(final String url, final long batchTimeInMs, final int batchCount, final String solverToUse,
              final boolean debugMode, final String fznFlags) {
        this.conn = setupDb();
        this.k8sApiUrl = url;
        this.batchTimeInMs = batchTimeInMs;
        this.batchCount = batchCount;
        final Conf conf = new Conf();
        conf.setProperty("solver", solverToUse);
        LOG.info("Configuring debug mode: {}", debugMode);
        if (debugMode) {
            conf.setProperty("debug_mode", "true");
        }
        conf.setProperty("fzn_flags", fznFlags);
        this.model = createDcmModel(conn, conf, relevantTables);
        this.puller = new DataPuller();
        puller.run(conn, k8sApiUrl);
        LOG.info("Initialized scheduler with batchCount:{} batchSize:{} model:{} relevantTables:{}", this.batchCount,
                this.batchTimeInMs, model, relevantTables);
    }

    /**
     * Instantiates a DCM model based on the configured policies.
     */
    private Model createDcmModel(final DSLContext conn, final Conf conf, final List<Table<?>> tables) {
        final File modelFile = new File("k8s_model.mzn");
        final File dataFile = new File("k8s_data.mzn");
        final List<String> policies = Policies.getDefaultPolicies();
        return Model.buildModel(conn, tables, policies, modelFile, dataFile, conf);
    }

    /**
     * Sets up a private, in-memory database.
     */
    private DSLContext setupDb() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        final InputStream resourceAsStream = this.getClass().getResourceAsStream("/scheduler_tables.sql");
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
}