/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.using;

class DBConnectionPool {
    private static final Settings JOOQ_SETTING = new Settings().withExecuteLogging(false);
    private final String databaseName;
    private final DataSource ds;

    DBConnectionPool() {
        this.databaseName = UUID.randomUUID().toString();
        setupBaseTables();
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:h2:mem:%s;LOG=0;UNDO_LOG=0", databaseName));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("maximumPoolSize", "20");
        this.ds = new HikariDataSource(config);
    }

    /**
     * Sets up a private, in-memory database.
     */
    void setupBaseTables() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/scheduler_tables.sql");
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            // Create a fresh database
            final String connectionURL = String.format("jdbc:h2:mem:%s;create=true;LOG=0", databaseName);
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.H2);
            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);
            semiColonSeparated.forEach(using::execute);
        } catch (final SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets up a private, in-memory database.
     */
    @VisibleForTesting
    DSLContext getConnectionToDb() {
        return using(ds, SQLDialect.H2, JOOQ_SETTING);
    }
}
