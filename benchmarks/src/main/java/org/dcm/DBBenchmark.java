/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */
package org.dcm;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import org.dcm.viewupdater.DDlogUpdater;
import org.dcm.viewupdater.Updater;
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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.sql.DriverManager.getConnection;
import static org.jooq.impl.DSL.using;

@State(Scope.Benchmark)
public class DBBenchmark {

    private DSLContext dbCtx;
    private Connection connection;
    private Updater updater;
    private Model model;
    private List<String> baseTables;

    @Param({"100", "1000", "10000", "100000"})
    public int numRecords;

    @Param({"H2", "HSQLDB"})
    public String db;

    private int index = 0;

    public static void main(String[] args) throws IOException, RunnerException {
        Options opts = new OptionsBuilder()
                .include(".*")
                .warmupIterations(2)
                .measurementIterations(5)
                .mode(Mode.AverageTime)
                .shouldDoGC(true)
                .result("profiling-result-using-prepared-stmts.csv").resultFormat(ResultFormatType.CSV)
                .forks(1)
                .build();

        new Runner(opts).run();
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
            connection = getConnection(connectionURL, properties);
            dbCtx = using(connection, SQLDialect.H2);
            dbCtx.execute("create schema curr");
            dbCtx.execute("set schema curr");

            init();

            baseTables = new ArrayList<>();
            baseTables.add("POD");
            baseTables.add("NODE");


//            for (final String entry : baseTables) {
//                final String tableName = entry.toUpperCase(Locale.US);
//                if (model.getIRTables().containsKey(tableName)) {
//                    final String triggerName = "TRIGGER_" + tableName;
//
//                    final StringBuilder builder = new StringBuilder();
//                    builder.append("CREATE TRIGGER " + triggerName + " " + "BEFORE INSERT ON " + tableName + " " +
//                            "FOR EACH ROW CALL \"" + Updater.class.getName() + "\"");
//
//                    final String command = builder.toString();
//                    dbCtx.execute(command);
//                }
//            }
//
            updater = new Updater(connection, dbCtx, baseTables,
                    new DDlogUpdater(r -> updater.receiveUpdateFromDDlog(r)), model.getIRTables());


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
            connection = getConnection(connectionURL, properties);
            dbCtx = using(connection, SQLDialect.HSQLDB);

            init();

            baseTables = new ArrayList<>();
            baseTables.add("POD");
            baseTables.add("NODE");

//            for (final String entry : baseTables) {
//                final String tableName = entry.toUpperCase(Locale.US);
//                if (model.getIRTables().containsKey(tableName)) {
//                    final String triggerName = "TRIGGER_" + tableName;
//
//                    final StringBuilder builder = new StringBuilder();
//                    builder.append("CREATE TRIGGER " + triggerName + " " + "BEFORE INSERT ON " + tableName + " " +
//                            "FOR EACH ROW CALL \"" + Updater.class.getName() + "\"");
//
//                    final String command = builder.toString();
//                    dbCtx.execute(command);
//                }
//            }

            updater = new Updater(connection, dbCtx, baseTables,
                    new DDlogUpdater(r -> updater.receiveUpdateFromDDlog(r)), model.getIRTables());


        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() {
        index += numRecords;

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

        dbCtx.execute("create table SPARECAPACITY " +
                "(\n" +
                "  name varchar(36) not null, " +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " +  ")"
        );

        model = buildModel(dbCtx, new ArrayList<>(), "testModel");
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
            default:
                // code block
        }
    }

    /*
     * Tears down connection to db.
     */
    @TearDown(Level.Invocation)
    public void teardown() throws SQLException {
        dbCtx.execute("drop table node");
        dbCtx.execute("drop table pod");
        dbCtx.execute("drop table sparecapacity");
        dbCtx.close();
    }

    @Benchmark
    public void insertRecords() {

        try {
        final PreparedStatement nodeStmt = connection.prepareStatement(
                "insert into node values(?, false, false, false, false, " +
                        "false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
        final PreparedStatement podStmt = connection.prepareStatement(
                "insert into pod values(?, 'scheduled', ?, 'default', 1, 1, 1, 1, 'owner', 'owner', 1)");

        for (int i = index; i < (numRecords + index); i++) {
            final String node = "node" + i;
            final String pod = "pod" + i;
            nodeStmt.setString(1, node);

            podStmt.setString(1, pod);
            podStmt.setString(2, node);

            nodeStmt.executeUpdate();
            podStmt.executeUpdate();
        }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        updater.flushUpdates();
    }

    @Benchmark
    public void insertRecordsWithNoTriggers() {
        final int largeNumber = 50000;
        final int start = index + largeNumber;
        final int end = (start + numRecords * 2);
        final String insertStatement = "insert into node values(?, false, false," +
                " false, false, false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)";
        try {
            final PreparedStatement nodeStmt = connection.prepareStatement(insertStatement);
            for (int i = start; i < end; i++) {
                nodeStmt.setString(1, "node" + i);
                nodeStmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        updater.flushUpdates();
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
