/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.jooq.impl.DSL.using;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests Weave's use of a constraint solver. Place the mzn files in src/text/java/.../resources folder to
 * access them with classLoader.getResource().
 */
public class ModelTest {
    static {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
    }

    @Test
    public void noopTest() {
        final String modelName = "noopConstraints";

        final DSLContext conn = setup();
        conn.execute("create table placement(groupId integer, hostId varchar(36))");

        final Model model = buildModel(conn, Collections.emptyList(), modelName);

        conn.execute("insert into placement values (1, 'h1')");
        conn.execute("insert into placement values (2, 'h2')");
        conn.execute("insert into placement values (3, 'h3')");
        conn.execute("insert into placement values (4, 'h4')");

        model.updateData();
        model.solveModel();

        final Result<Record> fetch = conn.selectFrom("curr.placement").fetch();
        assertEquals(4, fetch.size());
    }


    @Test
    public void controllableTest() {
        final String modelName = "controllableTest";

        final DSLContext conn = setup();
        conn.execute("create table placement(groupId integer, controllable__hostId varchar(36))");

        final Model model = buildModel(conn, Collections.emptyList(), modelName);

        conn.execute("insert into placement values (1, 'h1')");
        conn.execute("insert into placement values (2, 'h2')");
        conn.execute("insert into placement values (3, 'h3')");
        conn.execute("insert into placement values (4, 'h4')");

        model.updateData();
        model.solveModel();
    }


    @Test
    public void solveModelWithUpdateTest() {
        final String modelName = "noopConstraints";

        final DSLContext conn = setup();
        conn.execute("create table placement(groupId integer, hostId varchar(36))");

        final Model model = buildModel(conn, Collections.emptyList(), modelName);

        conn.execute("insert into placement values (1, 'h1')");
        conn.execute("insert into placement values (2, 'h2')");
        conn.execute("insert into placement values (3, 'h3')");
        conn.execute("insert into placement values (4, 'h4')");

        model.updateData();
        final Map<String, Result<? extends Record>> placement =
                model.solveModelWithoutTableUpdates(Sets.newHashSet("PLACEMENT"));
        assertEquals(1, placement.size());
    }


    @Test
    public void nullTest() {
        final String modelName = "nullConstraint";

        final DSLContext conn = setup();
        conn.execute("create table placement(groupId varchar(100))");

        final Model model = buildModel(conn, Collections.emptyList(), modelName);

        conn.insertInto(DSL.table("placement"))
            .values(Collections.singletonList(null)).execute();

        model.updateData();
        model.solveModel();

        final Result<Record> fetch = conn.selectFrom("curr.placement").fetch();
        assertEquals(1, fetch.size());
    }

    @Test
    public void longSolverTest() {
        // model and data files will use this as its name
        final String modelName = "longSolverTest";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table HOSTS(" +
                "HOST_ID varchar(36)," +
                "CONTROLLABLE__IN_SEGMENT boolean," +
                "PRIMARY KEY (HOST_ID)" +
                ")");
        conn.execute("create table STRIPES(" +
                "STRIPE_ID integer, " +
                "CONTROLLABLE__HOST_ID varchar(36)," +
                "PRIMARY KEY (STRIPE_ID, CONTROLLABLE__HOST_ID)," +
                "FOREIGN KEY(CONTROLLABLE__HOST_ID) REFERENCES HOSTS(HOST_ID)" +
                ")");

        // build model - fails when building for the first time
        final Model model = buildModel(conn, Collections.emptyList(), modelName);

        final int NUM_HOSTS = 20;
        final int NUM_STRIPES = 4;

        // insert hosts
        for (int i = 0; i < NUM_HOSTS; i++) {
            conn.execute("insert into HOSTS values ('h" + i + "', true)");
        }

        // insert stripes which do not use all the hosts (in this case h3)
        for (int i = 0; i < NUM_STRIPES; i++) {
            conn.execute("insert into STRIPES values (" + i + ",'h1')");
            conn.execute("insert into STRIPES values (" + i + ",'h2')");
            conn.execute("insert into STRIPES values (" + i + ",'h3')");
        }

        // update and solve
        model.updateData();
        model.solveModel();

        // TODO: missing the assert. What to expect when the solver can return multiple results?
    }

    @Test
    public void stringInForeignKey() {
        // model and data files will use this as its name
        final String modelName = "stringInForeignKey";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table HOSTS(" +
                "HOST_ID varchar(36)," +
                "CONTROLLABLE__IN_SEGMENT boolean," +
                "PRIMARY KEY (HOST_ID)" +
                ")");
        conn.execute("create table STRIPES(" +
                "STRIPE_ID integer, " +
                "CONTROLLABLE__HOST_ID varchar(36)," +
                "PRIMARY KEY (STRIPE_ID, CONTROLLABLE__HOST_ID)," +
                "FOREIGN KEY(CONTROLLABLE__HOST_ID) REFERENCES HOSTS(HOST_ID)" +
                ")");

        // wrong sql with ambiguous field
        final List<String> views = toListOfViews("" +
                "CREATE VIEW constraint_exclude_non_data_nodes1 AS " +
                "SELECT * FROM hosts JOIN stripes ON hosts.host_id = stripes.controllable__host_id " +
                "where hosts.controllable__in_segment != false OR stripes.controllable__host_id != hosts.host_id;\n" +

                "CREATE VIEW constraint_exclude_non_data_nodes2 AS " +
                "SELECT count(*) FROM hosts JOIN stripes ON hosts.host_id = stripes.controllable__host_id " +
                "group by hosts.host_id " +
                "having count(hosts.host_id) <= 2;\n"
        );
        // build model
        final Model model = buildModel(conn, views, modelName);

        // insert hosts
        conn.execute("insert into HOSTS values ('h1', true)");
        conn.execute("insert into HOSTS values ('h2', true)");
        conn.execute("insert into HOSTS values ('h3', true)");
        // insert stripes which do not use all the hosts (in this case h3)
        conn.execute("insert into STRIPES values (1,'h1')");
        conn.execute("insert into STRIPES values (1,'h2')");
        conn.execute("insert into STRIPES values (2,'h1')");
        conn.execute("insert into STRIPES values (2,'h2')");
        conn.execute("insert into STRIPES values (3,'h1')");
        conn.execute("insert into STRIPES values (3,'h2')");

        // update and solve
        model.updateData();
        model.solveModel();

        final List<?> results = conn.selectFrom("STRIPES")
                .fetch("CONTROLLABLE__HOST_ID");

        // check the size of the stripes
        assertTrue(results.contains("h3"));
    }


    @Test
    public void innerCountTest() {
        // model and data files will use this as its name
        final String modelName = "innerCountTest";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table HOSTS(" +
                "HOST_ID varchar(36)," +
                "CONTROLLABLE__IN_SEGMENT boolean," +
                "PRIMARY KEY (HOST_ID)" +
                ")");
        conn.execute("create table STRIPES(" +
                "STRIPE_ID integer, " +
                "CONTROLLABLE__HOST_ID varchar(36)," +
                "PRIMARY KEY (STRIPE_ID, CONTROLLABLE__HOST_ID)," +
                "FOREIGN KEY(CONTROLLABLE__HOST_ID) REFERENCES HOSTS(HOST_ID)" +
                ")");

        // wrong sql with ambiguous field
        final List<String> views = toListOfViews("" +
                "CREATE VIEW constraint_exclude_non_data_nodes2 AS " +
                "SELECT * FROM hosts JOIN stripes ON hosts.host_id = stripes.controllable__host_id " +
                "group by hosts.host_id " +
                "having count(hosts.host_id) <= 2;\n"
        );
        // build model
        final Model model = buildModel(conn, views, modelName);

        // insert hosts
        conn.execute("insert into HOSTS values ('h1', true)");
        conn.execute("insert into HOSTS values ('h2', true)");
        conn.execute("insert into HOSTS values ('h3', true)");
        // insert stripes which do not use all the hosts (in this case h3)
        conn.execute("insert into STRIPES values (1,'h1')");
        conn.execute("insert into STRIPES values (1,'h3')");
        conn.execute("insert into STRIPES values (2,'h1')");
        conn.execute("insert into STRIPES values (2,'h3')");
        conn.execute("insert into STRIPES values (3,'h1')");
        conn.execute("insert into STRIPES values (3,'h3')");

        // update and solve
        model.updateData();
        model.solveModel();

        final List<?> results = conn.selectFrom("STRIPES")
                .fetch("CONTROLLABLE__HOST_ID");

        // check the size of the stripes
        assertTrue(results.contains("h1"));
        assertTrue(results.contains("h2"));
        assertTrue(results.contains("h3"));
    }


    @Test
    public void innerSubqueryCountTest() {
        // model and data files will use this as its name
        final String modelName = "innerSubqueryCountTest";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table HOSTS(" +
                "HOST_ID varchar(36)," +
                "CONTROLLABLE__IN_SEGMENT boolean," +
                "PRIMARY KEY (HOST_ID)" +
                ")");
        conn.execute("create table STRIPES(" +
                "STRIPE_ID integer, " +
                "CONTROLLABLE__HOST_ID varchar(36)," +
                "PRIMARY KEY (STRIPE_ID, CONTROLLABLE__HOST_ID)," +
                "FOREIGN KEY(CONTROLLABLE__HOST_ID) REFERENCES HOSTS(HOST_ID)" +
                ")");

        // wrong sql with ambiguous field
        final List<String> views = toListOfViews("" +
                "CREATE VIEW constraint_x AS " +
                "SELECT * FROM hosts " +
                "where (select count(stripes.stripe_id) from stripes) >= 2;\n"
        );
        // build model
        final Model model = buildModel(conn, views, modelName);

        // insert hosts
        conn.execute("insert into HOSTS values ('h1', true)");
        conn.execute("insert into HOSTS values ('h2', true)");
        conn.execute("insert into HOSTS values ('h3', true)");
        // insert stripes which do not use all the hosts (in this case h3)
        conn.execute("insert into STRIPES values (1,'h1')");
        conn.execute("insert into STRIPES values (1,'h2')");
        conn.execute("insert into STRIPES values (2,'h1')");
        conn.execute("insert into STRIPES values (2,'h2')");
        conn.execute("insert into STRIPES values (3,'h1')");
        conn.execute("insert into STRIPES values (3,'h2')");

        // update and solve
        model.updateData();
        model.solveModel();

        final List<?> results = conn.selectFrom("STRIPES")
                .fetch("CONTROLLABLE__HOST_ID");

        // check the size of the stripes
        assertTrue(results.contains("h1") || results.contains("h2") || results.contains("h3"));
    }

    @Test
    public void ambiguousFieldsInViewTest() {
        // model and data files will use this as its name
        final String modelName = "ambiguousFieldsInViewTest";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        // wrong sql with ambiguous field
        final List<String> views = toListOfViews("" +
                "CREATE VIEW join_view AS " +
                "SELECT * FROM hosts JOIN epochs ON epoch_id = epochs.epoch_id;");

        // build model
        assertThrows(ModelException.class, () -> buildModel(conn, views, modelName));
    }

    @Test
    public void twoTableJoinInViewTest() {
        // model and data files will use this as its name
        final String modelName = "twoTableJoinInViewTest";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW join_view AS " +
                "SELECT * FROM hosts JOIN epochs ON hosts.epoch_id = epochs.epoch_id;");

        // insert data
        conn.execute("insert into epochs values (1)");
        conn.execute("insert into epochs values (2)");
        conn.execute("insert into epochs values (3)");

        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 1)");
        conn.execute("insert into HOSTS values ('h1', 2)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 2)");
        conn.execute("insert into HOSTS values ('h1', 3)");
        conn.execute("insert into HOSTS values ('h2', 3)");

        // build model - fails when building for the first time
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }

    @Test
    public void tableWithSubqueryInViewTest() {
        // model and data files will use this as its name
        final String modelName = "tableWithSubqueryInViewTest";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW join_view AS " +
                "SELECT hosts.epoch_id FROM hosts where hosts.epoch_id = (select max(epochs.epoch_id) from epochs)");

        // insert data
        conn.execute("insert into epochs values (1)");
        conn.execute("insert into epochs values (2)");
        conn.execute("insert into epochs values (3)");

        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 3)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testUnaryOperator() {
        // model and data files will use this as its name
        final String modelName = "testUnaryOperator";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE t1 (" +
                "c1 integer NOT NULL," +
                "controllable__c2 integer NOT NULL" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW constraint_t1 AS " +
                "SELECT * FROM t1 where controllable__c2 in (select c1 from t1 as A) and not(controllable__c2 = 1)");

        // insert data
        conn.execute("insert into t1 values (1, 1)");
        conn.execute("insert into t1 values (2, 1)");
        conn.execute("insert into t1 values (2, 1)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();

        final List<?> results = conn.selectFrom("T1")
                .fetch("CONTROLLABLE__C2");

        // check the size of the stripes
        assertFalse(results.contains(1));
    }


    @Test
    public void testAggregateWithMultiplePredicates() {
        // model and data files will use this as its name
        final String modelName = "testAggregateWithMultiplePredicates";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE t1 (" +
                "c1 integer NOT NULL," +
                "controllable__c2 integer NOT NULL" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW constraint_t1 AS " +
                "SELECT * FROM t1 group by c1 having sum(controllable__c2) = 5 and count(controllable__c2) = 1; ");

        // insert data
        conn.execute("insert into t1 values (1, 5)");
        conn.execute("insert into t1 values (2, 5)");
        conn.execute("insert into t1 values (3, 5)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();

        final List<?> results = conn.selectFrom("T1")
                .fetch("CONTROLLABLE__C2");

        // check the size of the stripes
        assertFalse(results.contains(1));
    }


    @Test
    public void testAggregateKubernetesBug() {
        // model and data files will use this as its name
        final String modelName = "testAggregateWithMultiplePredicates";

        // create database
        final DSLContext conn = setup();

        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  network_unavailable boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")\n");
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null primary key,\n" +
                "  status varchar(36) not null,\n" +
                "  node_name varchar(36) not null,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  ephemeral_storage_request bigint not null,\n" +
                "  pods_request bigint not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")\n");

        conn.execute("\n" +
                "create table labels_to_check_for_presence\n" +
                "(\n" +
                "  label_key varchar(100) not null,\n" +
                "  present boolean not null\n" +
                ")"
        );
        conn.execute("\n" +
                "-- Tracks the set of labels per node\n" +
                "create table node_labels\n" +
                "(\n" +
                "  node_name varchar(36) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  foreign key(node_name) references node_info(name)\n" +
                ")\n"
        );
        final List<String> views = toListOfViews("create view constraint_valid_nodes as\n" +
                "select * from pod_info\n" +
                "where\n" +
                "not(status = 'Pending') or\n" +
                "pod_info.controllable__node_name in\n" +
                "  (select node_labels.node_name from node_labels\n" +
                "   join labels_to_check_for_presence\n" +
                "        on node_labels.label_key = labels_to_check_for_presence.label_key\n" +
                "   group by node_labels.node_name\n" +
                "   having count(node_labels.label_key) = (select count(*) from labels_to_check_for_presence));");

        // build model
        buildModel(conn, views, modelName);
    }

    @Test
    public void viewOfViewWithoutControllableShouldNotBeVar() {
        // model and data files will use this as its name
        final String modelName = "viewOfViewWithoutControllableShouldNotBeVar";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW v1 AS " +
                "SELECT host_id as host_id, epoch_id as epoch_id FROM hosts where epoch_id = 1;" +
                "CREATE VIEW v2 AS " +
                "SELECT * FROM v1 where epoch_id = 1;");

        // insert data
        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 3)");

        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void negativeValueOfSelectedItem() {
        // model and data files will use this as its name
        final String modelName = "negativeValueOfSelectedItem";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "controllable__epoch_id integer NOT NULL," +
                "PRIMARY KEY (host_id, controllable__epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW objective_v1 AS " +
                "SELECT -count(host_id) as host_id FROM hosts where controllable__epoch_id = 1;");

        // insert data
        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 3)");

        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testExistsOperator() {
        // model and data files will use this as its name
        final String modelName = "testExistsOperator";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE t1 (" +
                "controllable__c1 integer NOT NULL" +
                ")"
        );
        conn.execute("CREATE TABLE t2 (" +
                "c1 integer NOT NULL" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW constraint_t1 AS " +
                "SELECT * FROM t1 where exists(select c1 from t2 where t2.c1 = t1.controllable__c1) = true;");

        // insert data
        conn.execute("insert into t1 values (2)");
        conn.execute("insert into t2 values (3)");

        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();

        final List<Integer> fetch = conn.selectFrom("t1").fetch("CONTROLLABLE__C1", Integer.class);
        assertEquals(1, fetch.size());
        assertEquals(3, fetch.get(0).intValue());
    }



    @Test
    public void createsOptVariable() {
        // model and data files will use this as its name
        final String modelName = "createsOptVariable";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "controllable__epoch_id bigint NOT NULL" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW v1 AS " +
                "SELECT * FROM hosts where controllable__epoch_id = 1;");

        // insert data
        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 3)");

        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }

    @Test
    public void testControllableInHead() {
        // model and data files will use this as its name
        final String modelName = "testControllableInHead";

        // create database
        final DSLContext conn = setup();

        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key\n" +
                ")\n");
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null primary key,\n" +
                "  status varchar(36) not null,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")\n");
        conn.execute("create table pod_ports_request\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  host_ip varchar(100) not null,\n" +
                "  host_port integer not null,\n" +
                "  host_protocol varchar(10) not null,\n" +
                "  foreign key(pod_name) references pod_info(pod_name)\n" +
                ")\n");

        final List<String> views = toListOfViews("create view pods_with_port_requests as\n" +
                "select pod_info.controllable__node_name as node_name,\n" +
                "       pod_ports_request.host_port as host_port,\n" +
                "       pod_ports_request.host_ip as host_ip,\n" +
                "       pod_ports_request.host_protocol as host_protocol\n" +
                "from pod_info\n" +
                "join pod_ports_request\n" +
                "     on pod_ports_request.pod_name = pod_info.pod_name\n" +
                "where pod_info.status = 'Pending'");

        conn.execute("insert into node_info values ('n1')");
        conn.execute("insert into pod_info values ('p1', 'Pending', 'n1')");
        conn.execute("insert into pod_ports_request values ('p1', '127.0.0.1', 1841, 'tcp')");

        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testControllableInJoin() {
        // model and data files will use this as its name
        final String modelName = "testControllableInJoin";

        // create database
        final DSLContext conn = setup();

        conn.execute("create table node_info\n" +
                "(\n" +
                "  name integer not null primary key,\n" +
                "  cpu_allocatable integer not null\n" +
                ")\n");
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null primary key,\n" +
                "  status varchar(36) not null,\n" +
                "  controllable__node_name integer not null,\n" +
                "  cpu_request integer not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")\n");

        final List<String> views = toListOfViews("create view pods_demand_per_node as\n" +
                "select (node_info.cpu_allocatable - sum(pod_info.cpu_request)) as cpu_utilization\n" +
                "from node_info\n" +
                "join pod_info\n" +
                "     on pod_info.controllable__node_name = node_info.name\n" +
                "where status = 'Pending'\n" +
                "group by node_info.name, node_info.cpu_allocatable;");

        conn.execute("insert into node_info values (1, 10)");
        conn.execute("insert into pod_info values ('p1', 'Pending', 1, 5)");

        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }

    @Test
    public void testForAllWithJoin() {
        final String modelName = "testForAllWithJoin";
        final DSLContext conn = setup();
        conn.execute("create table t1\n" +
                "(\n" +
                "  c1 integer not null primary key,\n" +
                "  controllable__c2 integer not null \n" +
                ")\n");
        conn.execute("create table t2\n" +
                "(\n" +
                "  c1 integer not null primary key" +
                ")");
        final String pod_info_constant = "create view constraint_ex as\n" +
                " select * from t1 join t2 on t1.c1 = t2.c1" +
                " where controllable__c2 = t1.c1";
        conn.execute("insert into t1 values (1, 1)");
        conn.execute("insert into t1 values (2, 1)");
        conn.execute("insert into t1 values (3, 1)");
        conn.execute("insert into t2 values (1)");
        conn.execute("insert into t2 values (2)");
        final Model model = buildModel(conn, Collections.singletonList(pod_info_constant), modelName);
        model.updateData();
        model.solveModel();
        final Result<Record> t1 = conn.selectFrom("t1").fetch();
        assertEquals(1, t1.get(0).get("CONTROLLABLE__C2"));
        assertEquals(2, t1.get(1).get("CONTROLLABLE__C2"));
        assertNotEquals(3, t1.get(2).get("CONTROLLABLE__C2"));
    }


    @Test
    public void testControllableInJoinLarge() {
        // model and data files will use this as its name
        final String modelName = "testControllableInJoinLarge";

        // create database
        final DSLContext conn = setup();

        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  cpu_allocatable integer not null\n" +
                ")\n");
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null primary key,\n" +
                "  status varchar(36) not null,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request integer not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")\n");

        final List<String> views = toListOfViews("create view pods_demand_per_node as\n" +
                "select (node_info.cpu_allocatable - sum(pod_info.cpu_request)) as cpu_utilization\n" +
                "from node_info\n" +
                "join pod_info\n" +
                "     on pod_info.controllable__node_name = node_info.name\n" +
                "where status = 'Pending'\n" +
                "group by node_info.name, node_info.cpu_allocatable;");

        for (int i = 0; i < 50; i++) {
            conn.execute(String.format("insert into node_info values ('n%s', 100000)", i));
        }
        for (int i = 0; i < 100; i++) {
            conn.execute(String.format("insert into pod_info values ('p%s', 'Pending', 'n1', 5)", i));
        }
        // Should not be opt
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void singleTableView() {
        // model and data files will use this as its name
        final String modelName = "singleTableView";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews("" +
                "CREATE VIEW nojoin_view AS " +
                "SELECT * FROM hosts where epoch_id = 1;");

        // insert data
        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 3)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }

    @Test
    public void subqueryDifferentContexts() {
        // model and data files will use this as its name
        final String modelName = "subqueryDifferentContexts";

        // create database
        final DSLContext conn = setup();
        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );


        final List<String> views = toListOfViews("" +
                "CREATE VIEW curr_epoch_rows AS " +
                "SELECT * " +
                "FROM hosts WHERE epoch_id = (" +
                "   SELECT MAX(epoch_id)" +
                "   FROM epochs" +
                ");");

        // insert data
        conn.execute("insert into epochs values (1)");
        conn.execute("insert into epochs values (2)");
        conn.execute("insert into epochs values (3)");

        conn.execute("insert into HOSTS values ('h1', 1)");
        conn.execute("insert into HOSTS values ('h2', 2)");
        conn.execute("insert into HOSTS values ('h3', 3)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testTableAliasGeneration() {
        // model and data files will use this as its name
        final String modelName = "testTableAliasGeneration";

        // create database
        final DSLContext conn = setup();
        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "failure_state varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews("CREATE VIEW non_constraint_rows AS " +
                "SELECT * " +
                "FROM hosts as X JOIN hosts as Y on X.host_id = Y.host_id " +
                "WHERE X.failure_state = 'UNRESPONSIVE' and Y.epoch_id = (" +
                "   SELECT MAX(epoch_id)" +
                "   FROM epochs" +
                ") having all_different(X.host_id) = true;");

        // insert data
        conn.execute("insert into epochs values (1)");
        conn.execute("insert into epochs values (2)");
        conn.execute("insert into epochs values (3)");

        conn.execute("insert into HOSTS values ('h1', 'UNRESPONSIVE', 1)");
        conn.execute("insert into HOSTS values ('h2', 'ACTIVE', 2)");
        conn.execute("insert into HOSTS values ('h3', 'ACTIVE', 3)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }

    @Disabled
    @Test
    public void testViewAliasGeneration() {
        // model and data files will use this as its name
        final String modelName = "testViewAliasGeneration";

        // create database
        final DSLContext conn = setup();
        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "failure_state varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );

        final List<String> views = toListOfViews(
                "CREATE VIEW subset AS " +
                        "SELECT hosts.epoch_id as epoch_id, " +
                        "       hosts.failure_state as failure_state, " +
                        "       hosts.host_id as host_id " +
                        "FROM hosts where epoch_id = 10;" +

                "CREATE VIEW objective_blah_rows AS " +
                "SELECT sum(X.epoch_id) as derp " +
                "FROM subset as X JOIN subset as Y on X.host_id = Y.host_id " +
                "WHERE X.failure_state = 'UNRESPONSIVE' and Y.epoch_id = (" +
                "   SELECT MAX(epoch_id)" +
                "   FROM epochs" +
                ") having all_different(X.host_id) = true;");

        // insert data
        conn.execute("insert into epochs values (1)");
        conn.execute("insert into epochs values (2)");
        conn.execute("insert into epochs values (3)");

        conn.execute("insert into HOSTS values ('h1', 'UNRESPONSIVE', 1)");
        conn.execute("insert into HOSTS values ('h2', 'ACTIVE', 2)");
        conn.execute("insert into HOSTS values ('h3', 'ACTIVE', 3)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testViewReference() {
        // model and data files will use this as its name
        final String modelName = "testViewReference";

        // create database
        final DSLContext conn = setup();
        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "failure_state varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)" +
                ")"
        );
        conn.execute("CREATE VIEW latest_epochs AS " +
                "SELECT * FROM epochs where epoch_id = 2");
        conn.execute("CREATE VIEW latest_hosts AS\n" +
                "SELECT hosts.* FROM hosts JOIN latest_epochs on hosts.epoch_id = latest_epochs.epoch_id");

        final List<String> views = toListOfViews("CREATE VIEW constraint_view AS " +
                                                      "SELECT * FROM latest_hosts where epoch_id = 2;");

        // insert data
        conn.execute("insert into epochs values (1)");
        conn.execute("insert into epochs values (2)");
        conn.execute("insert into epochs values (3)");

        conn.execute("insert into HOSTS values ('h1', 'UNRESPONSIVE', 1)");
        conn.execute("insert into HOSTS values ('h2', 'ACTIVE', 2)");
        conn.execute("insert into HOSTS values ('h3', 'ACTIVE', 3)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();

        // Should not be unsat.
    }


    @Test
    public void testStringLiteralInModelButNotInData() {
        // model and data files will use this as its name
        final String modelName = "testStringLiteralInModelButNotInData";

        // create database
        final DSLContext conn = setup();
        conn.execute("CREATE TABLE t1 (" +
                "c1 varchar(30) PRIMARY KEY" +
                ")"
        );
        final List<String> views = toListOfViews("CREATE VIEW constraint_c1 AS " +
                "SELECT * FROM t1 where c1 = 'non-existent-string' or c1 = 'some-other-string';");

        // insert data
        conn.execute("insert into t1 values ('some-other-string')");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testNegativeNumber() {
        // model and data files will use this as its name
        final String modelName = "testNegativeNumber";

        // create database
        final DSLContext conn = setup();
        conn.execute("CREATE TABLE t1 (" +
                "controllable__c1 integer PRIMARY KEY" +
                ")"
        );
        final List<String> views = toListOfViews("CREATE VIEW constraint_c1 AS " +
                "SELECT * FROM t1 where controllable__c1 >= -10 and controllable__c1 >= -20;");

        // insert data
        conn.execute("insert into t1 values (1)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();

        final List<Integer> fetch = conn.selectFrom("t1").fetch("CONTROLLABLE__C1", Integer.class);
        assertEquals(1, fetch.size());
        assertEquals(-10, fetch.get(0).intValue());
    }

    @Test
    public void testGroupByGeneration() {
        // model and data files will use this as its name
        final String modelName = "testGroupByGeneration";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null\n" +
                ")"
        );
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(36) not null primary key,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")"
        );

        final List<String> views = toListOfViews(
                "create view least_requested_sums as\n" +
                        "select sum(pod_info.cpu_request) as cpu_load\n" +
                        "       from pod_info join node_info on pod_info.cpu_request = node_info.cpu_allocatable " +
                        " group by node_info.name;"
        );

        conn.execute("insert into node_info values ('n1', 1, 1)");
        conn.execute("insert into node_info values ('n2', 10, 10)");
        conn.execute("insert into pod_info values ('p1', 'n1', 1, 2)");
        conn.execute("insert into pod_info values ('p2', 'n1', 1, 2)");
        conn.execute("insert into pod_info values ('p3', 'n2', 1, 2)");
        conn.execute("insert into pod_info values ('p4', 'n2', 2, 2)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        final Map<String, Result<? extends Record>> podInfo =
                model.solveModelWithoutTableUpdates(Collections.singleton("POD_INFO"));
        podInfo.get("POD_INFO").forEach(
                e -> assertEquals("n1", e.get("CONTROLLABLE__NODE_NAME"))
        );
    }


    @Test
    public void testAggregateGeneration() {
        // model and data files will use this as its name
        final String modelName = "testAggregateGeneration";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null\n" +
                ")"
        );
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(36) not null primary key,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")"
        );

        final List<String> views = toListOfViews(
                "create view least_requested_sums as\n" +
                "select sum(pod_info.cpu_request) as cpu_load\n" +
                "       from pod_info join node_info on pod_info.controllable__node_name = node_info.name " +
                " group by node_info.name;"
        );

        conn.execute("insert into node_info values ('n1', 1, 1)");
        conn.execute("insert into node_info values ('n2', 10, 10)");
        conn.execute("insert into pod_info values ('p1', 'n1', 2, 2)");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
    }


    @Test
    public void testMembership() {
        // model and data files will use this as its name
        final String modelName = "testMembership";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key\n" +
                ")"
        );
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(36) not null primary key,\n" +
                "  controllable__node_name varchar(36) not null\n" +
                ")"
        );

        final List<String> views = toListOfViews(
                "create view constraint_membership as\n" +
                        "select *\n" +
                        "from pod_info\n" +
                        "where pod_info.controllable__node_name in (select node_info.name from node_info);"
        );

        conn.execute("insert into node_info values ('n1')");
        conn.execute("insert into node_info values ('n2')");
        conn.execute("insert into pod_info values ('p1', 'blah')");

        // build model
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        model.solveModel();
        final Result<Record> podInfo = conn.selectFrom("POD_INFO").fetch();
        assertEquals(1, podInfo.size());
        assertTrue(podInfo.get(0).get("CONTROLLABLE__NODE_NAME").equals("n1") ||
                            podInfo.get(0).get("CONTROLLABLE__NODE_NAME").equals("n2"));
    }


    @Test
    public void testSelectExpression() {
        // model and data files will use this as its name
        final String modelName = "testSelectExpression";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null\n" +
                ")"
        );
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(36) not null primary key,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")"
        );

        final List<String> views = toListOfViews(
                "create view least_requested as\n" +
                    "select (sum(node_info.cpu_allocatable) - sum(pod_info.cpu_request)) as cpu_utilization," +
                    "       (sum(node_info.memory_allocatable) - sum(pod_info.memory_request)) as mem_utilization\n" +
                    "       from node_info\n" +
                    "       join pod_info\n" +
                    "            on pod_info.controllable__node_name = node_info.name\n" +
                    "       group by node_info.name;");


        for (int i = 0; i < 1; i++) {
            conn.execute(String.format("insert into node_info values ('n%s', 1000, 2000)", i));
        }
        for (int i = 0; i < 10; i++) {
            conn.execute(String.format("insert into pod_info values ('p%s', 'n0', 5, 10)", i));
        }

        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        final Map<String, Result<? extends Record>> results
                = model.solveModelWithoutTableUpdates(Collections.singleton("LEAST_REQUESTED"));
        assertEquals(0, results.size());
    }

    @Test
    public void testMultiColumnGroupBy() {
        // model and data files will use this as its name
        final String modelName = "testMultiColumnGroupBy";

        // create database
        final DSLContext conn = setup();
        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")"
        );
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(36) not null primary key,\n" +
                "  controllable__node_name varchar(36) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  ephemeral_storage_request bigint not null,\n" +
                "  pods_request bigint not null,\n" +
                "  foreign key(controllable__node_name) references node_info(name)\n" +
                ")"
        );


        final List<String> views = toListOfViews(
                        "    create view constraint__symmetry_breaking as\n" +
                        "    select *\n" +
                        "    from pod_info\n" +
                        "    group by cpu_request, memory_request, ephemeral_storage_request, pods_request\n" +
                        "    having increasing(controllable__node_name) = true;"
        );

        final Model model = buildModel(conn, views, modelName);
        model.updateData();
    }

    @Test
    public void testHavingClause() {
        final String modelName = "testHavingClause";
        // create database
        final DSLContext conn = setup();
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null primary key,\n" +
                "  status varchar(36) not null,\n" +
                "  controllable__node_name varchar(36) not null, \n" +
                "  namespace varchar(100) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  ephemeral_storage_request bigint not null,\n" +
                "  pods_request bigint not null,\n" +
                "  owner_name varchar(100) not null,\n" +
                "  creation_timestamp varchar(100) not null,\n" +
                "  priority integer not null,\n" +
                "  has_node_affinity boolean not null\n" +
                ")");

        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  isMaster boolean not null,\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  network_unavailable boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")");

        final List<String> views = toListOfViews("create view constraint_capacity as\n" +
            "select\n" +
            "  node_info.name as name\n" +
            "from\n" +
            "  node_info\n" +
            "  join pod_info on pod_info.controllable__node_name = node_info.name\n" +
            "group by\n" +
            "  node_info.name, node_info.cpu_allocatable, node_info.memory_allocatable, node_info.pods_allocatable\n" +
            "having\n" +
            "  sum(pod_info.cpu_request) < node_info.cpu_allocatable and\n" +
            "  sum(pod_info.memory_request) < node_info.memory_allocatable and\n" +
            "  sum(pod_info.pods_request) < node_info.pods_allocatable;");

        buildModel(conn, views, modelName);
    }

    @Test
    public void testAllQueries() {
        final String modelName = "testAllQueries";
        final DSLContext conn = setup();
        conn.execute("create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null primary key,\n" +
                "  status varchar(36) not null,\n" +
                "  controllable__node_name varchar(36) not null, \n" +
                "  namespace varchar(100) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  ephemeral_storage_request bigint not null,\n" +
                "  pods_request bigint not null,\n" +
                "  owner_name varchar(100) not null,\n" +
                "  creation_timestamp varchar(100) not null,\n" +
                "  priority integer not null,\n" +
                "  has_node_affinity boolean not null\n" +
                ")\n");

        conn.execute("create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null primary key,\n" +
                "  isMaster boolean not null,\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  network_unavailable boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")");

        conn.execute("create table pod_affinity_match_expressions\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  operator varchar(30) not null,\n" +
                "  topology_key varchar(100) not null,\n" +
                "  foreign key(pod_name) references pod_info(pod_name)\n" +
                ")\n");

        conn.execute("create table pod_ports_request\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  host_ip varchar(100) not null,\n" +
                "  host_port integer not null,\n" +
                "  host_protocol varchar(10) not null,\n" +
                "  foreign key(pod_name) references pod_info(pod_name)\n" +
                ")");

        conn.execute("create table pod_labels\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  is_selector boolean not null,\n" +
                "  foreign key(pod_name) references pod_info(pod_name)\n" +
                ")\n");

        conn.execute("create table pod_node_selector_labels\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  operator varchar(30) not null,\n" +
                "  foreign key(pod_name) references pod_info(pod_name)\n" +
                ")");

        conn.execute("create table node_labels\n" +
                "(\n" +
                "  node_name varchar(36) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  foreign key(node_name) references node_info(name)\n" +
                ")");

        conn.execute("create table pod_by_service\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  service_name varchar(100) not null,\n" +
                "  foreign key(pod_name) references pod_info(pod_name)\n" +
                ")");

        conn.execute("create table service_affinity_labels\n" +
                "(label_key varchar(100) not null\n)");

        insert_data(conn);

        final StringBuilder stringBuilder = new StringBuilder();
        final String pod_with_affinity_expr = "create view pod_with_affinity_expr as\n" +
                "  select distinct \n" +
                "  pod_info.pod_name as pod_name,\n" +
                "  pod_affinity_match_expressions.label_key as match_key,\n" +
                "  pod_affinity_match_expressions.label_value as match_value,\n" +
                "  pod_info.controllable__node_name as node_name\n" +
                "from pod_info join pod_affinity_match_expressions \n" +
                "on pod_info.pod_name = pod_affinity_match_expressions.pod_name\n";
        conn.execute(pod_with_affinity_expr);

        final String candidate_nodes_for_pods = "create view candidate_nodes_for_pods as\n" +
                "select pod_info.pod_name, node_labels.node_name\n" +
                "from pod_info\n" +
                "join pod_node_selector_labels\n" +
                "     on pod_info.pod_name = pod_node_selector_labels.pod_name\n" +
                "join node_labels\n" +
                "     on node_labels.label_key = pod_node_selector_labels.label_key\n" +
                "     and node_labels.label_value = pod_node_selector_labels.label_value";
        conn.execute(candidate_nodes_for_pods);

        final String pod_with_labels = "create view pod_with_labels  as\n" +
                "  select distinct \n" +
                "  pod_info.pod_name as pod_name,\n" +
                "  pod_labels.label_key as label_key,\n" +
                "  pod_labels.label_value as label_value,\n" +
                "  pod_info.controllable__node_name as node_name\n" +
                "from pod_info join pod_labels \n" +
                "on pod_info.pod_name = pod_labels.pod_name\n";
        conn.execute(pod_with_labels);

        final String pods_with_port_requests = "create view pods_with_port_requests as\n" +
                "select pod_info.controllable__node_name as node_name,\n" +
                "       pod_ports_request.host_port as host_port,\n" +
                "       pod_ports_request.host_ip as host_ip,\n" +
                "       pod_ports_request.host_protocol as host_protocol\n" +
                "from pod_info\n" +
                "join pod_ports_request\n" +
                "     on pod_ports_request.pod_name = pod_info.pod_name";
        conn.execute(pods_with_port_requests);

        final String services_with_affinity_labels = "create view services_with_affinity_labels as \n" +
                "select pod_by_service.service_name as service_name, pod_info.controllable__node_name" +
                " as node_name\n" +
                "from pod_by_service\n" +
                "join pod_info\n" +
                "on pod_info.pod_name = pod_by_service.pod_name\n" +
                "join pod_labels \n" +
                "on pod_info.pod_name = pod_labels.pod_name\n" +
                "join service_affinity_labels on \n" +
                "pod_labels.label_key = service_affinity_labels.label_key ";
        conn.execute(services_with_affinity_labels);

        final String constraint_node_predicates = "create view constraint_node_predicates as\n" +
                "select *\n" +
                "from pod_info\n" +
                "join node_info\n" +
                "     on pod_info.controllable__node_name = node_info.name\n" +
                "where node_info.unschedulable = false and\n" +
                "      node_info.memory_pressure = false and\n" +
                "      node_info.disk_pressure = false and\n" +
                "      node_info.pid_pressure = false and\n" +
                "      node_info.network_unavailable = false and\n" +
                "      node_info.ready = true";

        final String constraint_pod_to_pod_affinity = "create view constraint_pod_to_pod_affinity as \n" +
                "select * from pod_with_affinity_expr  join pod_with_labels on \n" +
                "pod_with_affinity_expr.match_key = pod_with_labels.label_key and \n" +
                "pod_with_affinity_expr.match_value = pod_with_labels.label_value and\n" +
                "pod_with_affinity_expr.pod_name != pod_with_labels.pod_name \n" +
                "where (\n" +
                "pod_with_affinity_expr.node_name in (\n" +
                "select pod_with_labels.node_name as node_name\n" +
                "from pod_with_labels as A join pod_with_affinity_expr as B on \n" +
                "B.match_key = A.label_key and \n" +
                "B.match_value = A.label_value and\n" +
                "B.pod_name != A.pod_name \n" +
                "where " +
                "pod_with_affinity_expr.match_key = pod_with_labels.label_key and \n" +
                "pod_with_affinity_expr.match_value = pod_with_labels.label_value and\n" +
                "pod_with_affinity_expr.pod_name != pod_with_labels.pod_name))";

        final String constraint_capacity = "create view constraint_capacity as\n" +
                "select\n" +
                "  node_info.name as name\n" +
                "from\n" +
                "  node_info\n" +
                "  join pod_info on pod_info.controllable__node_name = node_info.name\n" +
                "group by\n" +
                "  node_info.name, node_info.cpu_allocatable,\n" +
                "  node_info.memory_allocatable, node_info.pods_allocatable " +
                "having\n" +
                "  sum(pod_info.cpu_request) < node_info.cpu_allocatable and\n" +
                "  sum(pod_info.memory_request) < node_info.memory_allocatable and\n" +
                "  sum(pod_info.pods_request) < node_info.pods_allocatable\n";

        final String constraint_service_affinity_labels = "create view constraint_service_affinity_labels as\n" +
                "select *\n" +
                "from services_with_affinity_labels\n" +
                "join node_labels\n" +
                "     on node_labels.node_name = services_with_affinity_labels.node_name\n" +
                "group by services_with_affinity_labels.service_name\n" +
                "having all_equal(node_labels.label_value) = true";

        final String constraint_fk_constraint = "create view constraint_fk as\n" +
                "select * from pod_info where \n" +
                "pod_info.controllable__node_name in (select name from node_info)";

        stringBuilder.append(
                constraint_fk_constraint + ";" +
                        constraint_node_predicates + ";" +
                        constraint_pod_to_pod_affinity + ";" +
                        constraint_capacity + ";" +
                        constraint_service_affinity_labels
        );
        final List<String> views = toListOfViews(stringBuilder.toString());
        final Model model = buildModel(conn, views, modelName);
        model.updateData();
        assertThrows(ModelException.class, model::solveModel);
    }

    private void insert_data(final DSLContext conn) {
        for (int index = 1; index <= 10; index++) {
            String node = "null";
            if (index > 5) {
                node = "n6";
            }
            conn.execute("insert into pod_info values ('" + index + "', 'P', '" + node + "', 'default'," +
                    " 1, 1, 1, 1, '1', '1', 1, true)");
        }

        conn.execute("insert into node_info values ('n6', false, false, false, false, false, false, " +
                "true, false, 5, 5, 5, 5, 5, 5, 5, 5)");
        // valid node has no more space
        for (int index = 1; index <= 10; index++) {
            if (index == 6) {
                continue;
            }
            conn.execute("insert into node_info values ('n" + index + "', false, false, false, false, " +
                    "false, false, false, false, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)");
        } // other nodes that do have space are not allowed to have a pod residing on them

        for (int index = 1; index <= 10; index++) {
            String serviceNumber = "s1";
            String label = "label1";
            if (index > 5) {
                label = "label2";
                serviceNumber = "s2";
            }
            conn.execute("insert into pod_affinity_match_expressions values ('" + index + "', '" + label + "', " +
                    "'value1', '1', '1')");
            conn.execute("insert into pod_labels values ('" + index + "', '" + label + "', 'value1', true)");
            conn.execute("insert into pod_node_selector_labels values ('" + index + "', '" +
                    label + "', 'value1', true)");
            conn.execute("insert into node_labels values ('n" + index + "', '" + label + "', 'value1')");
            conn.execute("insert into pod_by_service values ('" + index + "', '" + serviceNumber + "')");
        }

        conn.execute("insert into service_affinity_labels values ('label1')");
        conn.execute("insert into service_affinity_labels values ('label2')");
    }


    @Test
    public void corfuModel() {
        // model and data files will use this as its name
        final String modelName = "corfuModel";

        // create database
        final DSLContext conn = setup();

        conn.execute("CREATE TABLE epochs (" +
                "epoch_id bigint PRIMARY KEY" +
                ")"
        );
        conn.execute("CREATE TABLE cluster_id (" +
                "cluster_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (cluster_id, epoch_id)" +
                ")"
        );
        conn.execute("CREATE TABLE hosts (" +
                "host_id varchar(36)," +
                "epoch_id bigint NOT NULL," +
                "failure_state varchar(36) NOT NULL," +
                "hostname varchar(36) NOT NULL, " +
                "controllable__is_layout_server boolean NOT NULL DEFAULT false," +
                "controllable__is_sequencer boolean NOT NULL DEFAULT false," +
                "controllable__in_segment boolean NOT NULL DEFAULT false," +
                "is_layout_server boolean NOT NULL DEFAULT false," +
                "is_sequencer boolean NOT NULL DEFAULT false," +
                "in_segment boolean NOT NULL DEFAULT false," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (host_id, epoch_id)," +
                "CHECK (failure_state IN ('ACTIVE', 'UNRESPONSIVE', 'HEALING'))" +
                ")"
        );
        conn.execute("CREATE TABLE segments (" +
                "segment_id varchar(36) NOT NULL," +
                "epoch_id bigint NOT NULL," +
                "replication_mode varchar(36) NOT NULL," +
                "segment_start bigint NOT NULL," +
                "segment_end bigint NOT NULL," +
                "FOREIGN KEY(epoch_id) REFERENCES epochs(epoch_id)," +
                "PRIMARY KEY (segment_id, epoch_id)," +
                "CHECK (replication_mode IN ('CHAIN_REPLICATION', 'QUORUM_REPLICATION', 'NO_REPLICATION'))" +
                ")"
        );
        conn.execute("CREATE TABLE stripes (" +
                "stripe_id varchar(36) NOT NULL," +
                "controllable__host_id varchar(36) NOT NULL," +
                "epoch_id bigint NOT NULL," +
                "segment_id varchar(36) NOT NULL," +
                "FOREIGN KEY (controllable__host_id, epoch_id) REFERENCES hosts(host_id, epoch_id)," +
                "FOREIGN KEY (segment_id, epoch_id) REFERENCES segments(segment_id, epoch_id)," +
                "PRIMARY KEY (stripe_id, controllable__host_id, epoch_id)" +
                ")"
        );

        // non-constraint views
        // TODO: this is the correct view but atm the compiler doesnt support that
        final List<String> views = toListOfViews("create view constraint_retain_old_values_hosts as\n" +
                "select * from hosts where epoch_id = (select max(epoch_id) from hosts as A) or\n" +
                "         (controllable__is_layout_server = is_layout_server and\n" +
                "         controllable__is_sequencer = is_sequencer and\n" +
                "         controllable__in_segment = in_segment);\n" +
                "\n" +
                "create view constraint_minimal_layouts as\n" +
                "select count(*) from hosts where epoch_id = (select max(epoch_id) from hosts as A)\n" +
                "         having sum(controllable__is_layout_server) >= 2;\n" +
                "\n" +
                "create view constraint_minimal_sequencers as\n" +
                "select count(*) from hosts where epoch_id = (select max(epoch_id) from hosts as A)\n" +
                "         having sum(controllable__is_sequencer) >= 2;\n" +
                "\n" +
                "create view constraint_minimal_segments as\n" +
                "select count(*) from hosts where epoch_id = (select max(epoch_id) from hosts as A)\n" +
                "         having sum(controllable__in_segment) >= 2;\n" +
                "\n" +
                "create view constraint_purge_policy as\n" +
                "select * from hosts where epoch_id = (select max(epoch_id) from hosts as A) and\n" +
                "         failure_state = 'UNRESPONSIVE' and\n" +
                "         controllable__is_layout_server = true and\n" +
                "         controllable__is_sequencer = true and\n" +
                "         controllable__in_segment = true");

        // build model
        buildModel(conn, views, modelName);
    }

    /**
     * @param sql
     * @return Splits by ';' separating the SQL for each view and returning a list of those views
     */
    private List<String> toListOfViews(final String sql) {
        return Splitter.on(";")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(sql);
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
    private Model buildModel(final DSLContext conn, final List<String> views, final String testName) {
        // get model file for the current test
        final File modelFile = new File("src/test/resources/" + testName + ".mzn");
        // create data file
        final File dataFile = new File("/tmp/" + testName + ".dzn");
        return Model.buildModel(conn, views, modelFile, dataFile);
    }

    /*
     * Sets up an in-memory Apache Derby database that we use for all tests.
     */
    private DSLContext setup() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            final Connection conn = getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.H2);
            using.execute("create schema curr");
            using.execute("set schema curr");
            return using;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @CanIgnoreReturnValue
    private Connection getConnection(final String url, final Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }
}
