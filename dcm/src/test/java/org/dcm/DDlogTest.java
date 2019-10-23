package org.dcm;

import org.dcm.viewupdater.H2Updater;
import org.dcm.viewupdater.ViewUpdater;
import org.jooq.DSLContext;
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

public class DDlogTest {

//   @Test
    //TODO: test does not fail with new hardcoded API
    // will fail with old API if not connected to the right ddlog program
    public void testMultipleModels() {

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
        //create model 1

        final String modelName1 = "model1";

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

        dbCtx.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " +  ")"
        );

        final List<String> baseTables1 = new ArrayList<>();
        baseTables1.add("POD");
        baseTables1.add("NODE");

        final Model model1 = buildModel(dbCtx, new ArrayList<>(), modelName1);

        final ViewUpdater updater1 = new H2Updater(modelName1, conn, dbCtx, model1.getIRTables(), baseTables1);

        // creating model 2
        final String modelName2 = "model2";

        dbCtx.execute("create table TESTONE (testid bigint not null)");
        dbCtx.execute("create table TESTTWO (testid bigint not null)");
        final List<String> baseTables = new ArrayList<>();
        baseTables.add("TESTONE");
        baseTables.add("TESTTWO");

        final Model model = buildModel(dbCtx, new ArrayList<>(), modelName2);
        final ViewUpdater updater = new H2Updater(modelName2, conn, dbCtx, model.getIRTables(), baseTables);

        // add updates for both model 1 and 2
        try {
            final PreparedStatement nodeStmt = conn.prepareStatement(
                    "insert into node values(?, false, false, false, false, " +
                            "false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
            final PreparedStatement podStmt = conn.prepareStatement(
                    "insert into pod values(?, 'scheduled', ?, 'default', 1, 1, 1, 1, 'owner', 'owner', 1)");

            final PreparedStatement testOneStmt = conn.prepareStatement(
                    "insert into testone values(?)");
            final PreparedStatement testTwoStmt = conn.prepareStatement(
                    "insert into testtwo values(?)");

            for (int j = 0; j < 10; j++) {
                int start = j * 10;
                final int end = (j + 1) * 10;
                for (; start < end; start++) {
                    final String node = "node" + start;
                    final String pod = "pod" + start;
                    nodeStmt.setString(1, node);

                    podStmt.setString(1, pod);
                    podStmt.setString(2, node);

                    nodeStmt.executeUpdate();
                    podStmt.executeUpdate();

                    testOneStmt.setInt(1, start);
                    testTwoStmt.setInt(1, start);

                    testOneStmt.executeUpdate();
                    testTwoStmt.executeUpdate();
                }
                updater1.flushUpdates();
                updater.flushUpdates();
            }
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

        dbCtx.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " +  ")"
        );

        final List<String> baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");

        final Model model = buildModel(dbCtx, new ArrayList<>(), modelName);

        final ViewUpdater updater = new H2Updater(modelName, conn, dbCtx, model.getIRTables(), baseTables);

        try {
            final PreparedStatement nodeStmt = conn.prepareStatement(
                    "insert into node values(?, false, false, false, false, " +
                            "false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
            final PreparedStatement podStmt = conn.prepareStatement(
                    "insert into pod values(?, 'scheduled', ?, 'default', 1, 1, 1, 1, 'owner', 'owner', 1)");

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
     *  - Build initial model (without hand-written constraints) on the first run. Note: the test FAILS on this scenario
     *  - Build the model with an already existing file. You should edit that file in between runs.
     *
     * @param dbCtx Connection to the DB
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