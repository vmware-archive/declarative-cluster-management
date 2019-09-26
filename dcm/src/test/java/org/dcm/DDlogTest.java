package org.dcm;

import org.dcm.viewupdater.H2Updater;
import org.dcm.viewupdater.ViewUpdater;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.jooq.impl.DSL.using;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DDlogTest {

    @Test
    public void testDeletesAndUpdates() {
        final String modelName = "modelName";
        final Connection conn;
        final DSLContext dbCtx;
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            conn = DriverManager.getConnection(connectionURL, properties);
            dbCtx = using(conn, SQLDialect.H2);
            dbCtx.execute("create schema curr");
            dbCtx.execute("set schema curr");
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        dbCtx.execute("create table NODE\n" +
                "(\n" +
                " name varchar(36) not null primary key, " +
                " unschedulable boolean not null, " +
                " out_of_disk boolean not null, " +
                " memory_pressure boolean not null, " +
                " disk_pressure boolean not null," +
                " pid_pressure boolean not null," +
                " ready boolean not null," +
                " network_unavailable boolean not null," +
                " cpu_capacity bigint not null," +
                " memory_capacity bigint not null," +
                " ephemeral_storage_capacity bigint not null," +
                " pods_capacity bigint not null," +
                " cpu_allocatable bigint not null," +
                " memory_allocatable bigint not null," +
                " ephemeral_storage_allocatable bigint not null," +
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
                " priority integer not null,  " +
                " schedulerName varchar(50),\n" +
                " has_node_selector_labels boolean not null,\n" +
                " has_pod_affinity_requirements boolean not null, " +
                " has_pod_anti_affinity_requirements boolean not null)"
        );

        dbCtx.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " + ")"
        );

        final List<String> baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");

        final Model model = buildModel(dbCtx, new ArrayList<>(), modelName);

        final ViewUpdater updater = new H2Updater(conn, dbCtx, model.getIRTables(), baseTables);

        try {
            final PreparedStatement nodeStmt = conn.prepareStatement(
                    "insert into node values(?, false, false, false, false, " +
                            "false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
            final PreparedStatement podStmt = conn.prepareStatement(
                    "insert into pod values(?, 'Pending', ?, 'default', 1, 1, 1, 1, 'owner', " +
                            "'owner', 1, 'dcm-scheduler', true, true, true)");
            for (int j = 0; j < 1; j++) {
                final int numRecords = 10;
                int index = j * numRecords;
                final int iEnd = index + numRecords;
                for (; index < iEnd; index++) {

                    final String node = "node" + index;
                    final String pod = "pod" + index;
                    nodeStmt.setString(1, node);

                    podStmt.setString(1, pod);
                    podStmt.setString(2, node);

                    nodeStmt.executeUpdate();
                    podStmt.executeUpdate();
                }
                updater.flushUpdates();
            }

            for (int j = 0; j < 1; j++) {
                final int numRecords = 10;
                int index = j * numRecords;
                final int iEnd = index + numRecords;
                for (; index < iEnd; index++) {
                    final String node = "node" + index;
                    final String pod = "pod" + index;
                    dbCtx.execute("delete from node where name = '" + node + "'");
                    dbCtx.execute("update pod set cpu_request = 5 where pod_name = '" + pod + "'");
                }
                updater.flushUpdates();
            }
            final Result<? extends Record> result =
                    dbCtx.resultQuery("select count(*) as num from sparecapacity").fetch();

            assertTrue((long) result.getValue(0, "NUM", 10) == 0L);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEndToEnd() {
        final String modelName = "modelName";
        final Connection conn;
        final DSLContext dbCtx;
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            conn = DriverManager.getConnection(connectionURL, properties);
            dbCtx = using(conn, SQLDialect.H2);
            dbCtx.execute("create schema curr");
            dbCtx.execute("set schema curr");
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        dbCtx.execute("create table NODE\n" +
                "(\n" +
                " name varchar(36) not null primary key, " +
                " unschedulable boolean not null, " +
                " out_of_disk boolean not null, " +
                " memory_pressure boolean not null, " +
                " disk_pressure boolean not null," +
                " pid_pressure boolean not null," +
                " ready boolean not null," +
                " network_unavailable boolean not null," +
                " cpu_capacity bigint not null," +
                " memory_capacity bigint not null," +
                " ephemeral_storage_capacity bigint not null," +
                " pods_capacity bigint not null," +
                " cpu_allocatable bigint not null," +
                " memory_allocatable bigint not null," +
                " ephemeral_storage_allocatable bigint not null," +
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
                " priority integer not null,  " +
                " schedulerName varchar(50),\n" +
                " has_node_selector_labels boolean not null,\n" +
                " has_pod_affinity_requirements boolean not null, " +
                " has_pod_anti_affinity_requirements boolean not null)"
        );

        dbCtx.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " + ")"
        );

        final List<String> baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");

        final Model model = buildModel(dbCtx, new ArrayList<>(), modelName);
        final ViewUpdater updater = new H2Updater(conn, dbCtx, model.getIRTables(), baseTables);

        try {
            final PreparedStatement nodeStmt = conn.prepareStatement(
                    "insert into node values(?, false, false, false, false, " +
                            "false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
            final PreparedStatement podStmt = conn.prepareStatement(
                    "insert into pod values(?, 'Pending', ?, 'default', 1, 1, 1, 1, 'owner', " +
                            "'owner', 1, 'dcm-scheduler', true, true, true)");

            for (int j = 0; j < 50; j++) {
                final int numRecords = 10;
                int index = j * numRecords;
                final int iEnd = index + numRecords;
                for (; index < iEnd; index++) {

                    final String node = "node" + index;
                    final String pod = "pod" + index;
                    nodeStmt.setString(1, node);

                    podStmt.setString(1, pod);
                    podStmt.setString(2, node);

                    nodeStmt.executeUpdate();
                    podStmt.executeUpdate();
                }
                updater.flushUpdates();
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds a Model for a test case, making sure to:
     * - Build initial model (without hand-written constraints) on the first run. Note: the test FAILS on this scenario
     * - Build the model with an already existing file. You should edit that file in between runs.
     *
     * @param dbCtx    Connection to the DB
     * @param testName Name of the test case. Model and data files will be based on that name
     * @return built Model
     */
    private Model buildModel(final DSLContext dbCtx, final List<String> views, final String testName) {
        final File modelFile = new File("src/test/resources/" + testName + ".mzn");
        // create data file
        final File dataFile = new File("/tmp/" + testName + ".dzn");

        return Model.buildModel(dbCtx, views, modelFile, dataFile);
    }
}