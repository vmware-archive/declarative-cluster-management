/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */
package org.dcm;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import org.dcm.viewupdater.DerbyUpdater;
import org.dcm.viewupdater.H2Updater;
import org.dcm.viewupdater.HSQLUpdater;
import org.dcm.viewupdater.PGUpdater;
import org.dcm.viewupdater.ViewUpdater;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.sql.DriverManager.getConnection;
import static org.jooq.impl.DSL.using;

@State(Scope.Benchmark)
public class DBBenchmark {

    private DSLContext dbCtx;
    private ViewUpdater viewUpdater;
    private Model model;
    private List<String> baseTables;

    @Param({"100", "1000", "10000", "100000"})
    public int numRecords;

    @Param({"H2", "HQSLDB", "DERBY", "POSTGRES"})
    public String db;

    public static void main(String[] args) throws IOException, RunnerException {
        Options opts = new OptionsBuilder()
                .include(".*")
                .warmupIterations(1)
                .measurementTime(TimeValue.seconds(5))
                .measurementIterations(1)
                .mode(Mode.Throughput)
                .shouldDoGC(true)
                .result("profiling-result.csv").resultFormat(ResultFormatType.CSV)
                .forks(1)
                .build();

        new Runner(opts).run();
    }

    private void setupDerby() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // The following block ensures we always drop the database between tests
            try {
                final String dropUrl = "jdbc:derby:memory:test;drop=true";
                getConnection(dropUrl, properties);
            } catch (final SQLException e) {
                // We could not drop a database because it was never created. Move on.
            }
            // Create a fresh database
            System.out.println("Derby Iteration");
            final String connectionURL = "jdbc:derby:memory:db;create=true";
            final Connection conn = getConnection(connectionURL, properties);
            dbCtx = using(conn, SQLDialect.DERBY);
            dbCtx.execute("create schema curr");
            dbCtx.execute("set schema curr");

            init();

            ViewUpdater.irTables = model.getIRTables();
            viewUpdater = new DerbyUpdater(dbCtx, baseTables);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Sets up an in-memory H2 database that we use for all tests.
     */
    private void setupH2() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            final Connection conn = getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.H2);
            using.execute("create schema curr");
            using.execute("set schema curr");

            dbCtx = using(conn, SQLDialect.H2);

            init();

            ViewUpdater.irTables = model.getIRTables();
            viewUpdater = new H2Updater(dbCtx, baseTables);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Sets up an in-memory HSQLDB database that we use for all tests.
     */
    public void setupHSQLDB() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:hsqldb:mem:db";
            final Connection conn = getConnection(connectionURL, properties);
            dbCtx = using(conn, SQLDialect.HSQLDB);

            init();

            ViewUpdater.irTables = model.getIRTables();
            viewUpdater = new HSQLUpdater(dbCtx, baseTables);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() {
        dbCtx.execute("create table NODE\n" +
                "(\n" +
                " name varchar(36) not null primary key, "  +
                " is_master boolean not null, "  +
                " unschedulable boolean not null, "  +
                " out_of_disk boolean not null, "  +
                " memory_pressure boolean not null, "  +
                " disk_pressure boolean not null,"  +
                " pid_pressure boolean not null,"  +
                " ready boolean not null,"  +
                " network_unavailable boolean not null,"  +
                " cpu_capacity bigint not null,"  +
                " memory_capacity bigint not null,"  +
                " ephemeral_storage_capacity bigint not null,"  +
                " pods_capacity bigint not null,"  +
                " cpu_allocatable bigint not null,"  +
                " memory_allocatable bigint not null,"  +
                " ephemeral_storage_allocatable bigint not null,"  +
                " pods_allocatable bigint not null)"
        );
        dbCtx.execute("create table POD\n" +
                "(\n" +
                " pod_name varchar(100) not null primary key,\n" +
                " status varchar(36) not null,\n" +
                " node_name varchar(36) not null,\n" +
                " namespace varchar(100) not null,\n" +
                " cpu_request bigint not null,\n" +
                " memory_request bigint not null,\n" +
                " ephemeral_storage_request bigint not null,\n" +
                " pods_request bigint not null,\n" +
                " owner_name varchar(100) not null,\n" +
                " creation_timestamp varchar(100) not null,\n" +
                " priority integer not null)"
        );

        model = buildModel(dbCtx, new ArrayList<>(), "testModel");

        dbCtx.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " +  ")"
        );

        baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");
    }

    @Setup(Level.Invocation)
    public void setupDB(){
        switch(db) {
            case "H2":
                setupH2();
                break;
            case "HSQLDB":
                setupHSQLDB();
                break;
            case "DERBY":
                setupDerby();
                break;
            case "POSTGRES":
                setupPostgres();
                break;
            default:
                // code block
        }
    }

    /*
     * Tears down up an in-memory HSQLDB database.
     */
    @TearDown(Level.Invocation)
    public void teardownHSQLDB() {
        dbCtx.execute("drop table node");
        dbCtx.execute("drop table pod");
        dbCtx.execute("drop table sparecapacity");
    }

    /*
     * Sets up an in-memory Postgres database that we use for all tests.
     */
    private void setupPostgres() {
        try {
            final Connection conn = DriverManager.getConnection("jdbc:pgsql://127.0.0.1:5432/test");

            final Statement statement = conn.createStatement();
            statement.executeUpdate("drop schema public cascade;");
            statement.executeUpdate("create schema public;");
            statement.close();

            dbCtx = using(conn, SQLDialect.POSTGRES);
            init();

            ViewUpdater.irTables = model.getIRTables();
            viewUpdater = new PGUpdater(conn, dbCtx, baseTables);

        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void insertRecords() {
        for (int i = 0; i < numRecords; i++) {
            dbCtx.execute("insert into node values('node" + i + "', false, false, false, false, " +
                    "false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
            dbCtx.execute("insert into pod values('pod" + i + "', 'scheduled', " +
                    "'node" + i + "', 'default', 1, 1, 1, 1, 'owner', 'owner', 1)");
        }
        viewUpdater.flushUpdates();
    }

    @CanIgnoreReturnValue
    private Model buildModel(final DSLContext dslCtx, final List<String> views, final String testName) {
        // get model file for the current test
        final File modelFile = new File("src/test/resources/" + testName + ".mzn");
        // create data file
        final File dataFile = new File("/tmp/" + testName + ".dzn");
        return Model.buildModel(dslCtx, views, modelFile, dataFile);
    }
}
