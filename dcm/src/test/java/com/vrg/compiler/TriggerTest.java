package com.vrg.compiler;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.Model;
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

    public static void capitalize(final String... input) {
        final StringBuilder builder = new StringBuilder();
        builder.append("Length of array: ").append(input.length);
        int counter = 0;
        for (final String i: input) {
            builder.append(" [").append(counter).append("]: ").append(i.trim());
            counter = counter + 1;
        }
        System.out.println(builder.toString());
    }

    @Test
    public void simpleExample() {
        final DSLContext conn = setup();

        conn.execute("create table temp (test varchar(100) not null primary key, id int not null)");
        conn.execute("select * from temp");

        conn.execute("CREATE procedure capitalize_columns (vals varchar(100) ...) " +
                "LANGUAGE JAVA  " +
                "PARAMETER STYLE DERBY " +
                "NO SQL  " +
                "EXTERNAL NAME 'com.vrg.compiler.TriggerTest.capitalize'");

        final String table = "TEMP";
        conn.execute("CREATE TRIGGER  capitalize_columns_trigger " +
                "AFTER INSERT ON " + table + " " +
                "REFERENCING NEW AS newTable " +
                "FOR EACH ROW " +
                "CALL capitalize_columns( " +
                "cast(cast('" + table + "' as char(38)) as varchar(100))," +
                "cast(cast(newTable.test as char(38)) as varchar(100))," +
                "cast(cast(newTable.id as char(38)) as varchar(100)))");

        conn.execute("insert into temp values ('test1', 1)");
        conn.execute("insert into temp values ('test2', 2)");
    }

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
    public void testSelectExpression() {
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

        final Model model = buildWeaveModel(conn, new ArrayList<>(), "testModel");

        conn.execute("insert into node values('node1', true, 1)");
        conn.execute("insert into node values('node2', true, 1)");
        conn.execute("insert into pod values('pod1', 'node1', 1)");
        conn.execute("insert into pod values('pod2', 'node2', 1)");
        model.updateData();
    }

    @CanIgnoreReturnValue
    private Connection getConnection(final String url, final Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }

    /**
     * Builds a Weave Model for a test case, making sure to:
     *  - Build initial model (without hand-written constraints) on the first run. Note: the test FAILS on this scenario
     *  - Build the model with an already existing file. You should edit that file in between runs.
     *
     * @param conn Connection to the DB
     * @param testName Name of the test case. Model and data files will be based on that name
     * @return built Model
     */
    @CanIgnoreReturnValue
    private Model buildWeaveModel(final DSLContext conn, final List<String> views, final String testName) {
        // get model file for the current test
        final File modelFile = new File("src/test/resources/" + testName + ".mzn");
        // create data file
        final File dataFile = new File("/tmp/" + testName + ".dzn");
        return Model.buildModel(conn, views, modelFile, dataFile);
    }

}
