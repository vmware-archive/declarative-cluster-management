/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;

import javax.sql.DataSource;
import java.util.UUID;

import static org.jooq.impl.DSL.using;

class DBConnectionPool implements IConnectionPool {
    private static final Settings JOOQ_SETTING = new Settings().withExecuteLogging(false);
    private final String databaseName;
    private final DataSource ds;

    DBConnectionPool() {
        this.databaseName = UUID.randomUUID().toString();
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:h2:mem:%s;LOG=0;UNDO_LOG=0", databaseName));
        config.addDataSourceProperty("foreign_keys", "true");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("maximumPoolSize", "20");
        this.ds = new HikariDataSource(config);
        DBViews.getSchema().forEach(getConnectionToDb()::execute);
    }

    /**
     * Sets up a private, in-memory database.
     */
    @Override
    @VisibleForTesting
    public DSLContext getConnectionToDb() {
        return using(ds, SQLDialect.H2, JOOQ_SETTING);
    }

    /**
     * Used only for refreshing the DB state between tests
     */
    @Override
    @VisibleForTesting
    public void refresh() {
        getConnectionToDb().execute("drop all objects");
        DBViews.getSchema().forEach(getConnectionToDb()::execute);
    }

    @Override
    @VisibleForTesting
    public DSLContext getDataConnectionToDb() {
        return getConnectionToDb();
    }
}
