package org.dcm;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.List;

import static org.jooq.impl.DSL.using;

public class TriggerTest {

    private DSLContext setup() {
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
            final String connectionURL = "jdbc:derby:memory:db;create=true";
            final Connection conn = getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.DERBY);
            using.execute("create schema curr");
            using.execute("set schema curr");
            return using;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLargerExample() {
        final DSLContext conn = setup();

        conn.execute("create table NODE\n" +
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
        conn.execute("create table POD\n" +
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

        conn.execute("create table SPARECAPACITY\n" +
                "(\n" +
                "  name varchar(36) not null,\n" +
                "  cpu_remaining bigint not null,  " +
                "  memory_remaining bigint not null, " +
                "  pods_remaining bigint not null " +  ")"
        );

        final List<String> baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");

        Model model = buildModel(conn, new ArrayList<>(), "testModel", true, baseTables);

        for (int j = 1; j < 6; j++) {
            long start = System.nanoTime();
            int delta = 100000;
            int i = j * delta;
            int iEnd = i + delta;
            for (; i < iEnd; i++) {
                conn.execute("insert into node values('node" + i + "', false, false, false, false, false, false, false, false, 1, 1, 1, 1, 1, 1, 1, 1)");
                conn.execute("insert into pod values('pod" + i + "', 'scheduled', 'node" + i + "', 'default', 1, 1, 1, 1, 'owner', 'owner', 1)");
            }
            long end = System.nanoTime();
            System.out.println("Time to receive updates: " + (end-start));
            model.updateData();
        }

    }


    @Test
    public void testSimpleExample() {
        final DSLContext conn = setup();

        conn.execute("create table NODE\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  unschedulable boolean not null,\n" +
                "  cpu_capacity bigint not null\n" +
                ")"
        );
        conn.execute("create table pod\n" +
                "(\n" +
                "  pod_name varchar(36) not null primary key,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request bigint not null\n" +
                ")"
        );

        conn.execute("create table USABLENODES\n" +
                "(\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  foreign key(controllable__node_name) references node(name)" +
                ")"
        );

        final List<String> baseTables = new ArrayList<>();
        baseTables.add("POD");
        baseTables.add("NODE");
        final Model model = buildModel(conn, new ArrayList<>(), "testModel", true, baseTables);

        conn.execute("insert into node values('node1', true, 1)");
        conn.execute("insert into node values('node2', true, 1)");

        conn.execute("insert into pod values('pod1', 'node1', 1)");
        conn.execute("insert into pod values('pod2', 'node2', 1)");

        conn.execute("insert into node values('node3', true, 1)");
        conn.execute("insert into pod values('pod3', 'node1', 1)");
        model.updateData();
    }

    @CanIgnoreReturnValue
    private Connection getConnection(final String url, final Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }

    /**
     * Builds a Model for a test case, making sure to:
     *  - Build initial model (without hand-written constraints) on the first run. Note: the test FAILS on this scenario
     *  - Build the model with an already existing file. You should edit that file in between runs.
     *
     * @param conn Connection to the DB
     * @param testName Name of the test case. Model and data files will be based on that name
     * @return built Model
     */
    @CanIgnoreReturnValue
    private Model buildModel(final DSLContext conn, final List<String> views, final String testName,
                             final boolean useDDlog, final List<String> list) {
        // get model file for the current test
        final File modelFile = new File("src/test/resources/" + testName + ".mzn");
        // create data file
        final File dataFile = new File("/tmp/" + testName + ".dzn");
        return Model.buildModel(conn, views, modelFile, dataFile, useDDlog, list);
    }
}
