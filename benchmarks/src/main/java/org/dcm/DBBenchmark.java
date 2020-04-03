/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class DBBenchmark {
//    private List<String> baseTables;
//    private List<String> views;
//
//    @Nullable private DSLContext dbCtx;
//    @Nullable private Connection connection;
//    @Nullable private ViewUpdater updater;
//    @Nullable private Model model;
//
//    @Nullable private PreparedStatement nodeStmt;
//    @Nullable private PreparedStatement podStmt;
//    @Nullable private PreparedStatement podNodeSelectorLabelsStmt;
//    @Nullable private PreparedStatement podPortsRequestStmt;
//    @Nullable private PreparedStatement containerHostPortsStmt;
//    @Nullable private PreparedStatement podsAffinityMatchExpressionsStmt;
//    @Nullable private PreparedStatement podsAntiAffinityMatchExpressionsStmt;
//    @Nullable private PreparedStatement podLabelsStmt;
//    @Nullable private PreparedStatement nodeLabelsStmt;
//    @Nullable private PreparedStatement volumeLabelsStmt;
//    @Nullable private PreparedStatement podByServiceStmt;
//    @Nullable private PreparedStatement nodeTaintsStmt;
//    @Nullable private PreparedStatement podTolerationsStmt;
//    @Nullable private PreparedStatement nodeImagesStmt;
//    @Nullable private PreparedStatement podImagesStmt;
//
//    @Param({"100"})
//    public int numRecords;
//
//    @Param({"100", "1000"})
//    public int numNodes;
//
//    @Param({"H2"})
//    public String db;
//
//    // proportion of how many pods should be pending
//    @Param({"25", "50"})
//    public double pendingPods;
//
//    // order: "In", "NotIn", "Exists", "DoesNotExist"
//    @Param({"25 25 25 25", "50 50 0 0", "50 0 50 0"})
//    public String labelProportions;
//
//    @Param({"1", "3", "5", "7"})
//    public double iterations;
//
//    @Param({"true", "false"})
//    public boolean useDDlog;
//
//    @Param({"true", "false"})
//    public boolean exerciseDeletesAndUpdates;
//
//    private int index = 0;
//    final String[] labels = {"In", "NotIn", "Exists", "DoesNotExist"};
//    double[] ranges = new double [4];
//
//    public DBBenchmark() {
//        baseTables = new ArrayList<>();
//        views = new ArrayList<>();
//        numRecords = 0;
//        db = "H2";
//        labelProportions = "";
//    }
//
//    public static void main(final String[] args) throws RunnerException {
//        final Options opts = new OptionsBuilder()
//                .include(".*")
//                .warmupIterations(2)
//                .measurementIterations(5)
//                .mode(Mode.AverageTime)
//                .shouldDoGC(true)
//                .result("profiling-result-index.csv").resultFormat(ResultFormatType.CSV)
//                .forks(1)
//                .build();
//
//        new Runner(opts).run();
//    }
//
//    @Setup(Level.Invocation)
//    public void setupDB() {
//        final List<String> proportions = Splitter.on(' ').splitToList(labelProportions);
//
//        ranges[0] = Double.parseDouble(proportions.get(0)) / 100;
//        ranges[1] = ranges[0] + Double.parseDouble(proportions.get(1)) / 100;
//        ranges[2] = ranges[1] + Double.parseDouble(proportions.get(2)) / 100;
//        ranges[3] = ranges[2] + Double.parseDouble(proportions.get(3)) / 100;
//
//        if (ranges[3] != 1) {
//            throw new RuntimeException("Ratios of Label operators In, NotIn," +
//                    " Exists and DoesNotExist do not sum up to 1");
//        }
//
//        if (pendingPods > 100 || pendingPods < -1) {
//            throw new RuntimeException("The ratio of pods that are scheduled should be > 0 and <= 100");
//        }
//
//        switch (db) {
//            case "H2":
//                setupH2();
//                break;
//            case "HSQLDB":
//                setupHSQLDB();
//                break;
//            default: // code block
//        }
//    }
//
//    /*
//     * Sets up an in-memory H2 database that we use for all tests.
//     */
//    private void setupH2() {
//
//        final Properties properties = new Properties();
//        properties.setProperty("foreign_keys", "true");
//        try {
//            final String connectionURL = "jdbc:h2:mem:;create=true";
//            connection = getConnection(connectionURL, properties);
//            dbCtx = using(connection, SQLDialect.H2);
//            dbCtx.execute("create schema curr");
//            dbCtx.execute("set schema curr");
//
//            init();
//
//            model = buildModel(dbCtx, new ArrayList<>(), "testModel");
//
//            if (useDDlog) {
//                updater = new H2Updater(connection, dbCtx, model.getIRTables(), baseTables);
//            } else {
//                updater = null;
//            }
//            createPreparedQueries();
//            createNodes();
//        } catch (final SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    /*
//     * Sets up an in-memory HSQLDB database that we use for all tests.
//     */
//    public void setupHSQLDB() {
//
//        final Properties properties = new Properties();
//        properties.setProperty("foreign_keys", "true");
//        try {
//            // Create a fresh database
//            final String connectionURL = "jdbc:hsqldb:mem:db";
//            connection = getConnection(connectionURL, properties);
//            dbCtx = using(connection, SQLDialect.HSQLDB);
//            dbCtx.execute("drop schema if exists curr cascade");
//            dbCtx.execute("create schema curr");
//            dbCtx.execute("set schema curr");
//
//            updater = new HSQLUpdater(connection, dbCtx, model.getIRTables(), baseTables);
//            model = buildModel(dbCtx, new ArrayList<>(), "testModel");
//
//            init();
//
//            model = buildModel(dbCtx, new ArrayList<>(), "testModel");
//
//            if (useDDlog) {
//                updater = new HSQLUpdater(connection, dbCtx, model.getIRTables(), baseTables);
//            } else {
//                updater = null;
//            }
//            createPreparedQueries();
//            createNodes();
//        } catch (final SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public void createBaseTables(final DSLContext dbCtx) {
//        dbCtx.execute("create table NODE\n" +
//                "(\n" +
//                " name varchar(36) not null primary key, " +
//                " unschedulable boolean not null, " +
//                " out_of_disk boolean not null, " +
//                " memory_pressure boolean not null, " +
//                " disk_pressure boolean not null," +
//                " pid_pressure boolean not null," +
//                " ready boolean not null," +
//                " network_unavailable boolean not null," +
//                " cpu_capacity bigint not null," +
//                " memory_capacity bigint not null," +
//                " ephemeral_storage_capacity bigint not null," +
//                " pods_capacity bigint not null," +
//                " cpu_allocatable bigint not null," +
//                " memory_allocatable bigint not null," +
//                " ephemeral_storage_allocatable bigint not null," +
//                " pods_allocatable bigint not null)"
//        );
//
//        dbCtx.execute("create table POD\n" +
//                "(\n" +
//                " pod_name varchar(100) not null primary key,\n" +
//                " status varchar(36) not null,\n" +
//                " node_name varchar(36) not null,\n" +
//                " namespace varchar(100) not null,\n" +
//                " cpu_request bigint not null,\n" +
//                " memory_request bigint not null,\n" +
//                " ephemeral_storage_request bigint not null,\n" +
//                " pods_request bigint not null,\n" +
//                " owner_name varchar(100) not null,\n" +
//                " creation_timestamp varchar(100) not null,\n" +
//                " priority integer not null,  " +
//                " schedulerName varchar(50),\n" +
//                " has_node_selector_labels boolean not null,\n" +
//                " has_pod_affinity_requirements boolean not null, " +
//                " has_pod_anti_affinity_requirements boolean not null)"
//        );
//
//        dbCtx.execute("create table PODPORTSREQUEST\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  host_ip varchar(100) not null,\n" +
//                "  host_port integer not null,\n" +
//                "  host_protocol varchar(10) not null \n" +
//                ")");
//
//        dbCtx.execute("create table CONTAINERHOSTPORTS\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  node_name varchar(36) not null,\n" +
//                "  host_ip varchar(100) not null,\n" +
//                "  host_port integer not null,\n" +
//                "  host_protocol varchar(10) not null" +
//                ")");
//
//        dbCtx.execute("create table PODNODESELECTORLABELS\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  term bigint not null,\n" +
//                "  match_expression bigint not null,\n" +
//                "  num_match_expressions bigint not null,\n" +
//                "  label_key varchar(100) not null,\n" +
//                "  label_operator varchar(100) null, " +
//                "  label_value varchar(100) not null\n" +
//                ")");
//
//        dbCtx.execute("create table PODAFFINITYMATCHEXPRESSIONS\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  label_selector bigint not null,\n" +
//                "  match_expression bigint not null,\n" +
//                "  num_match_expressions bigint not null,\n" +
//                "  label_key varchar(100) not null,\n" +
//                "  label_operator varchar(30) not null,\n" +
//                "  label_value varchar(36) not null,\n" +
//                "  topology_key varchar(100) not null" +
//                ")");
//
//        dbCtx.execute("create table PODANTIAFFINITYMATCHEXPRESSIONS\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  label_selector bigint not null,\n" +
//                "  match_expression bigint not null,\n" +
//                "  num_match_expressions bigint not null,\n" +
//                "  label_key varchar(100) not null,\n" +
//                "  label_operator varchar(30) not null,\n" +
//                "  label_value varchar(36) not null,\n" +
//                "  topology_key varchar(100) not null" +
//                ")");
//
//        dbCtx.execute("create table PODLABELS\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  label_key varchar(100) not null,\n" +
//                "  label_value varchar(36) not null\n" +
//                ")");
//
//        dbCtx.execute("create table NODELABELS\n" +
//                "(\n" +
//                "  node_name varchar(36) not null,\n" +
//                "  label_key varchar(100) not null,\n" +
//                "  label_value varchar(36) not null" +
//                ")");
//
//        dbCtx.execute("create table VOLUMELABELS\n" +
//                "(\n" +
//                "  volume_name varchar(36) not null,\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  label_key varchar(100) not null,\n" +
//                "  label_value varchar(36) not null" +
//                ")");
//
//        dbCtx.execute("create table PODBYSERVICE\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  service_name varchar(100) not null" +
//                ")");
//
//        dbCtx.execute("create table SERVICEAFFINITYLABELS\n" +
//                "(\n" +
//                "  label_key varchar(100) not null\n" +
//                ")");
//
//        dbCtx.execute("create table NODETAINTS\n" +
//                "(\n" +
//                "  node_name varchar(36) not null,\n" +
//                "  taint_key varchar(100) not null,\n" +
//                "  taint_value varchar(100),\n" +
//                "  taint_effect varchar(100) not null" +
//                ")");
//
//        dbCtx.execute("create table PODTOLERATIONS\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  tolerations_key varchar(100),\n" +
//                "  tolerations_value varchar(100),\n" +
//                "  tolerations_effect varchar(100),\n" +
//                "  tolerations_operator varchar(100)\n" +
//                ")");
//
//        dbCtx.execute("create table NODEIMAGES\n" +
//                "(\n" +
//                "  node_name varchar(36) not null,\n" +
//                "  image_name varchar(100) not null,\n" +
//                "  image_size bigint not null" +
//                ")");
//
//        dbCtx.execute("create table PODIMAGES\n" +
//                "(\n" +
//                "  pod_name varchar(100) not null,\n" +
//                "  image_name varchar(200) not null" +
//                ")");
//
//        dbCtx.execute("create table BATCHSIZE\n" +
//                "(\n" +
//                "  pendingPodsLimit integer not null primary key\n" +
//                ")");
//
//        dbCtx.execute("insert into BATCHSIZE values(5)");
//        dbCtx.execute("insert into SERVICEAFFINITYLABELS values('label_key')");
//    }
//
//    public void createViewsHelper(final DSLContext dbCtx) {
//        dbCtx.execute("create view podstoassignnolimit as\n" +
//                "select\n" +
//                "  pod_name,\n" +
//                "  status,\n" +
//                "  node_name as controllable__node_name,\n" +
//                "  namespace,\n" +
//                "  cpu_request,\n" +
//                "  memory_request,\n" +
//                "  ephemeral_storage_request,\n" +
//                "  pods_request,\n" +
//                "  owner_name,\n" +
//                "  creation_timestamp,\n" +
//                "  has_node_selector_labels,\n" +
//                "  has_pod_affinity_requirements,\n" +
//                "  has_pod_anti_affinity_requirements\n" +
//                "from pod\n" +
//                "where status = 'Pending' and node_name = 'null' and schedulerName = 'dcm-scheduler'\n" +
//                "order by creation_timestamp");
//
//        dbCtx.execute("create view podstoassign as\n" +
//                "select * from podstoassignnolimit limit 100");
//
//        dbCtx.execute("create view podswithportrequests as\n" +
//                "select podstoassign.controllable__node_name as controllable__node_name,\n" +
//                "       podportsrequest.host_port as host_port,\n" +
//                "       podportsrequest.host_ip as host_ip,\n" +
//                "       podportsrequest.host_protocol as host_protocol\n" +
//                "from podstoassign\n" +
//                "join podportsrequest\n" +
//                "     on podportsrequest.pod_name = podstoassign.pod_name\n");
//
//
//
//        dbCtx.execute("create view podnodeselectormatches as\n" +
//                "select podstoassign.pod_name as pod_name,\n" +
//                "       nodelabels.node_name as node_name\n" +
//                "from podstoassign\n" +
//                "join podnodeselectorlabels\n" +
//                "     on podstoassign.pod_name = podnodeselectorlabels.pod_name\n" +
//                "join nodelabels\n" +
//                "        on\n" +
//                "           (podnodeselectorlabels.label_operator = 'In'\n" +
//                "            and podnodeselectorlabels.label_key = nodelabels.label_key\n" +
//                "            and podnodeselectorlabels.label_value = nodelabels.label_value)\n" +
//                "        or (podnodeselectorlabels.label_operator = 'Exists'\n" +
//                "            and podnodeselectorlabels.label_key = nodelabels.label_key)\n" +
//                "        or (podnodeselectorlabels.label_operator = 'NotIn')\n" +
//                "        or (podnodeselectorlabels.label_operator = 'DoesNotExist')\n" +
//                "group by podstoassign.pod_name,  nodelabels.node_name, podnodeselectorlabels.term,\n" +
//                "         podnodeselectorlabels.label_operator, podnodeselectorlabels.num_match_expressions\n" +
//                "having case podnodeselectorlabels.label_operator\n" +
//                "            when 'NotIn'\n" +
//                "                 then not(any(podnodeselectorlabels.label_key = nodelabels.label_key\n" +
//                "                              and podnodeselectorlabels.label_value = nodelabels.label_value))\n" +
//                "            when 'DoesNotExist'\n" +
//                "                 then not(any(podnodeselectorlabels.label_key = nodelabels.label_key))\n" +
//                "            else count(distinct match_expression) = podnodeselectorlabels.num_match_expressions\n" +
//                "       end\n");
//
//        dbCtx.execute(
//                "create view interpodaffinitymatchesinner as\n" +
//                        "select podstoassign.pod_name as pod_name,\n" +
//                        "       podlabels.pod_name as matches,\n" +
//                        "       pod.node_name as node_name\n" +
//                        "from podstoassign\n" +
//                        "join podaffinitymatchexpressions\n" +
//                        "     on podstoassign.pod_name = podaffinitymatchexpressions.pod_name\n" +
//                        "join podlabels\n" +
//                        "        on (podaffinitymatchexpressions.label_operator = 'In'\n" +
//                        "            and podaffinitymatchexpressions.label_key = podlabels.label_key\n" +
//                        "            and podaffinitymatchexpressions.label_value = podlabels.label_value)\n" +
//                        "        or (podaffinitymatchexpressions.label_operator = 'Exists'\n" +
//                        "            and podaffinitymatchexpressions.label_key = podlabels.label_key)\n" +
//                        "        or (podaffinitymatchexpressions.label_operator = 'NotIn')\n" +
//                        "        or (podaffinitymatchexpressions.label_operator = 'DoesNotExist')\n" +
//                        "join pod\n" +
//                        "        on podlabels.pod_name = pod.pod_name\n" +
//                        "group by podstoassign.pod_name,  podlabels.pod_name, " +
//                        " podaffinitymatchexpressions.label_selector,\n" +
//                        "         podaffinitymatchexpressions.topology_key, " +
//                        " podaffinitymatchexpressions.label_operator,\n" +
//                        "         podaffinitymatchexpressions.num_match_expressions, pod.node_name\n" +
//                        "having case podaffinitymatchexpressions.label_operator\n" +
//                        "             when 'NotIn'\n" +
//                        "                  then not(any(podaffinitymatchexpressions.label_key " +
//                        "   = podlabels.label_key\n" +
//                        "                               and podaffinitymatchexpressions.label_value" +
//                        "   = podlabels.label_value))\n" +
//                        "             when 'DoesNotExist'\n" +
//                        "                  then not(any(podaffinitymatchexpressions.label_key " +
//                        "   = podlabels.label_key))\n" +
//                        "             else count(distinct match_expression) " +
//                        "   = podaffinitymatchexpressions.num_match_expressions\n" +
//                        "       end");
//
//        dbCtx.execute(
//                "create view interpodaffinitymatches as select *, count(*) " +
//                        "over (partition by pod_name) as num_matches from interpodaffinitymatchesinner\n");
//
//        dbCtx.execute(
//                "create view interpodantiaffinitymatchesinner as\n" +
//                        "select podstoassign.pod_name as pod_name,\n" +
//                        "       podlabels.pod_name as matches,\n" +
//                        "       pod.node_name as node_name\n" +
//                        "from podstoassign\n" +
//                        "join podantiaffinitymatchexpressions\n" +
//                        "     on podstoassign.pod_name = podantiaffinitymatchexpressions.pod_name\n" +
//                        "join podlabels\n" +
//                        "        on (podantiaffinitymatchexpressions.label_operator = 'In'\n" +
//                        "            and podantiaffinitymatchexpressions.label_key = podlabels.label_key\n" +
//                        "            and podantiaffinitymatchexpressions.label_value = podlabels.label_value)\n" +
//                        "        or (podantiaffinitymatchexpressions.label_operator = 'Exists'\n" +
//                        "            and podantiaffinitymatchexpressions.label_key = podlabels.label_key)\n" +
//                        "        or (podantiaffinitymatchexpressions.label_operator = 'NotIn')\n" +
//                        "        or (podantiaffinitymatchexpressions.label_operator = 'DoesNotExist')\n" +
//                        "join pod\n" +
//                        "        on podlabels.pod_name = pod.pod_name\n");
//
//        dbCtx.execute("create view interpodantiaffinitymatches as " +
//                "select *, count(*) over (partition by pod_name) as num_matches " +
//                " from interpodantiaffinitymatchesinner\n");
//
//        dbCtx.execute(
//                "create view sparecapacity as\n" +
//                        "select node.name as name,\n" +
//                        "   cast(node.cpu_allocatable - sum(pod.cpu_request) as integer) as cpu_remaining,\n" +
//                      "   cast(node.memory_allocatable - sum(pod.memory_request) as integer) as memory_remaining,\n" +
//                        "   cast(node.pods_allocatable - sum(pod.pods_request) as integer) as pods_remaining\n" +
//                        "from node\n" +
//                        "join pod\n" +
//                        "     on pod.node_name = node.name and pod.node_name != 'null'\n" +
//                        "group by node.name, node.cpu_allocatable,\n" +
//                        "         node.memory_allocatable, node.pods_allocatable");
//
//        dbCtx.execute(
//                "create view podsthattoleratenodetaints as\n" +
//                        "select podstoassign.pod_name as pod_name,\n" +
//                        "       A.node_name as node_name\n" +
//                        "from podstoassign\n" +
//                        "join podtolerations\n" +
//                        "     on podstoassign.pod_name = podtolerations.pod_name\n" +
//                      "join (select *, count(*) over (partition by node_name) as num_taints from nodetaints) as A\n" +
//                        "     on podtolerations.tolerations_key = A.taint_key\n" +
//                        "     and (podtolerations.tolerations_effect = null\n" +
//                        "          or podtolerations.tolerations_effect = A.taint_effect)\n" +
//                        "     and (podtolerations.tolerations_operator = 'Exists'\n" +
//                        "          or podtolerations.tolerations_value = A.taint_value)\n" +
//                        "group by podtolerations.pod_name, A.node_name, A.num_taints\n" +
//                        "having count(*) = A.num_taints\n");
//
//        dbCtx.execute("create view nodesthathavetolerations as\n select distinct node_name from nodetaints");
//    }
//
//    private void createIndices(final DSLContext dbCtx) {
//        dbCtx.execute("create index pod_info_idx on pod(status, node_name)");
//        dbCtx.execute("create index pod_node_selector_labels_fk_idx on podnodeselectorlabels (pod_name)");
//        dbCtx.execute("create index node_labels_idx on nodelabels (label_key, label_value)");
//        dbCtx.execute("create index pod_affinity_match_expressions_idx on podaffinitymatchexpressions (pod_name)");
//        dbCtx.execute("create index pod_labels_idx on podlabels (label_key, label_value)");
//    }
//
//    private void createViews(final DSLContext dbCtx) {
//        if (useDDlog) {
//            createViewsAsTables(dbCtx);
//        } else {
//            createViewsHelper(dbCtx);
//        }
//    }
//
//    private void createViewsAsTables(final DSLContext dbCtx) {
//        dbCtx.execute("create table SPARECAPACITY\n" +
//                "(\n" +
//                "  name varchar(36) not null,\n" +
//                "  cpu_remaining bigint not null,  " +
//                "  memory_remaining bigint not null, " +
//                "  pods_remaining bigint not null " + ")"
//        );
//
//        dbCtx.execute("create table PODSTOASSIGNNOLIMIT\n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " status varchar(36) not null,\n" +
//                " controllable__node_name varchar(36) not null,\n" +
//                " namespace varchar(100) not null,\n" +
//                " cpu_request bigint not null,\n" +
//                " memory_request bigint not null,\n" +
//                " ephemeral_storage_request bigint not null,\n" +
//                " pods_request bigint not null,\n" +
//                " owner_name varchar(100) not null,\n" +
//                " creation_timestamp varchar(100) not null,\n" +
//                " has_node_selector_labels boolean not null," +
//                " has_pod_affinity_requirements boolean not null)"
//        );
//
//        dbCtx.execute("create table PODSTOASSIGN\n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " status varchar(36) not null,\n" +
//                " controllable__node_name varchar(36) not null,\n" +
//                " namespace varchar(100) not null,\n" +
//                " cpu_request bigint not null,\n" +
//                " memory_request bigint not null,\n" +
//                " ephemeral_storage_request bigint not null,\n" +
//                " pods_request bigint not null,\n" +
//                " owner_name varchar(100) not null,\n" +
//                " creation_timestamp varchar(100) not null,\n" +
//                " has_node_selector_labels boolean not null," +
//                " has_pod_affinity_requirements boolean not null)"
//        );
//
//        dbCtx.execute("create table PODSWITHPORTREQUESTS\n" +
//                "(\n" +
//                " controllable__node_name varchar(100) not null,\n" +
//                "  host_ip varchar(100) not null,\n" +
//                "  host_port integer not null,\n" +
//                "  host_protocol varchar(10) not null \n)"
//        );
//
//        dbCtx.execute("create table PODNODESELECTORMATCHES \n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                "  node_name varchar(100) not null\n)"
//        );
//
//        dbCtx.execute("create table INTERPODAFFINITYMATCHESINNER \n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " matches varchar(100) not null,\n" +
//                " node_name varchar(100) not null\n)"
//        );
//
//        dbCtx.execute("create table INTERPODAFFINITYMATCHES \n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " matches varchar(100) not null,\n" +
//                " node_name varchar(100) not null," +
//                " num_matches bigint not null \n)"
//        );
//
//        dbCtx.execute("create table INTERPODANTIAFFINITYMATCHESINNER \n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " matches varchar(100) not null,\n" +
//                " node_name varchar(100) not null\n)"
//        );
//
//        dbCtx.execute("create table INTERPODANTIAFFINITYMATCHES \n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " matches varchar(100) not null,\n" +
//                " node_name varchar(100) not null," +
//                " num_matches bigint not null \n)"
//        );
//
//        dbCtx.execute("create table PODSTHATTOLERATENODETAINTS \n" +
//                "(\n" +
//                " pod_name varchar(100) not null,\n" +
//                " node_name varchar(100) not null\n)"
//        );
//
//        dbCtx.execute("create table NODESTHATHAVETOLERATIONS \n" +
//                "(\n" +
//                " node_name varchar(100) not null\n)"
//        );
//
//        dbCtx.execute("create index INTERPODAFFINITYMATCHES_idx on " +
//                "INTERPODAFFINITYMATCHES (pod_name, matches, node_name, num_matches)");
//        dbCtx.execute("create index INTERPODANTIAFFINITYMATCHES_idx on " +
//                "INTERPODANTIAFFINITYMATCHES (pod_name, matches, node_name, num_matches)");
//    }
//
//    private void init() {
//        index = 0;
//        baseTables = new ArrayList<>();
//
//        baseTables.add("POD");
//        baseTables.add("NODE");
//        baseTables.add("PODPORTSREQUEST");
//        baseTables.add("CONTAINERHOSTPORTS");
//        baseTables.add("PODNODESELECTORLABELS");
//        baseTables.add("PODAFFINITYMATCHEXPRESSIONS");
//        baseTables.add("PODANTIAFFINITYMATCHEXPRESSIONS");
//        baseTables.add("PODLABELS");
//        baseTables.add("NODELABELS");
//        baseTables.add("VOLUMELABELS");
//        baseTables.add("PODBYSERVICE");
//        baseTables.add("SERVICEAFFINITYLABELS");
//        baseTables.add("NODETAINTS");
//        baseTables.add("PODTOLERATIONS");
//        baseTables.add("NODEIMAGES");
//        baseTables.add("PODIMAGES");
//        baseTables.add("BATCHSIZE");
//
//        views.add("SPARECAPACITY");
//        views.add("PODSTOASSIGNNOLIMIT");
//        views.add("PODSTOASSIGN");
//        views.add("PODSWITHPORTREQUESTS");
//        views.add("PODNODESELECTORMATCHES");
//        views.add("INTERPODAFFINITYMATCHESINNER");
//        views.add("INTERPODAFFINITYMATCHES");
//        views.add("INTERPODANTIAFFINITYMATCHESINNER");
//        views.add("INTERPODANTIAFFINITYMATCHES");
//        views.add("PODSTHATTOLERATENODETAINTS");
//        views.add("NODESTHATHAVETOLERATIONS");
//
//        createBaseTables(dbCtx);
//        createIndices(dbCtx);
//        createViews(dbCtx);
//    }
//
//    private void createPreparedQueries() {
//        try {
//            nodeStmt = connection.prepareStatement(
//                    "insert into NODE values(?, false, false, false, false, " +
//                            "false, false, false, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)");
//
//            podStmt = connection.prepareStatement("insert into POD values(?, ?, ?, 'default', 1, 1, 1, 1, " +
//                    "'owner', 'owner', 1, 'dcm-scheduler', true, true, true)");
//
//            podNodeSelectorLabelsStmt = connection.prepareStatement(
//                    "insert into PODNODESELECTORLABELS values(?, 1, 1, 1, ?, ?, ?)");
//
//            podPortsRequestStmt = connection.prepareStatement(
//                    "insert into PODPORTSREQUEST values(?, 'ip', 8080, 'http')");
//
//            containerHostPortsStmt = connection.prepareStatement(
//                    "insert into CONTAINERHOSTPORTS values(?, ?, 'ip', 8080, 'http')");
//
//            podsAffinityMatchExpressionsStmt = connection.prepareStatement(
//                    "insert into PODAFFINITYMATCHEXPRESSIONS values(?, 1, 1, 1, ?, ?, ?, 'key')");
//
//            podsAntiAffinityMatchExpressionsStmt =
//                    connection.prepareStatement("insert into PODANTIAFFINITYMATCHEXPRESSIONS " +
//                            "values(?, 1, 1, 1, ?, ?, ?, 'key')");
//
//            podLabelsStmt = connection.prepareStatement("insert into PODLABELS values(?, ?, ?)");
//
//            nodeLabelsStmt = connection.prepareStatement("insert into NODELABELS values(?, ?, ?)");
//
//            volumeLabelsStmt = connection.prepareStatement("insert into VOLUMELABELS values('v1', ?, ?, ?)");
//
//            podByServiceStmt = connection.prepareStatement("insert into PODBYSERVICE values(?, 's1')");
//
//            nodeTaintsStmt = connection.prepareStatement("insert into NODETAINTS values(?, ?, ?, 'effect')");
//
//            podTolerationsStmt = connection.prepareStatement(
//                    "insert into PODTOLERATIONS values(?, ?, ?, 'effect', ?)");
//
//            nodeImagesStmt = connection.prepareStatement("insert into NODEIMAGES values(?, 'image', 10)");
//
//            podImagesStmt = connection.prepareStatement("insert into PODIMAGES values(?, 'image')");
//        } catch (final SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    private void createNodes() {
//        try {
//            for (int i = 0; i < numNodes; i++) {
//                final String node = "node" + i;
//                final String labelKey = "label_key" + i;
//                final String labelValue = "label_value" + i;
//                final String taintKey = "taint" + i;
//                final String taintValue = "taint" + i;
//                nodeStmt.setString(1, node);
//
//                nodeLabelsStmt.setString(1, node);
//                nodeLabelsStmt.setString(2, labelKey);
//                nodeLabelsStmt.setString(3, labelValue);
//
//                nodeTaintsStmt.setString(1, node);
//                nodeTaintsStmt.setString(2, taintKey);
//                nodeTaintsStmt.setString(3, taintValue);
//
//                nodeImagesStmt.setString(1, node);
//
//                nodeStmt.executeUpdate();
//                nodeLabelsStmt.executeUpdate();
//                nodeTaintsStmt.executeUpdate();
//                nodeImagesStmt.executeUpdate();
//            }
//
//            if (useDDlog) {
//                updater.flushUpdates();
//            }
//        } catch (final SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    private String getLabelOperator(final String[] labels, final double[] ranges, final double prop) {
//        for (int i = 0; i < ranges.length; i++) {
//            if (i > 0 && (prop > ranges[i - 1] && prop < ranges[i])) {
//                return labels[i];
//            } else if (prop < ranges[i]) {
//                return labels[i];
//            }
//        }
//        return "";
//    }
//
//    @Benchmark
//    public void insertRecordsIntoMultipleViews() {
//        for (int i = 0; i < iterations; i++) {
//            final int start = index + numRecords * (i + 1);
//            final int end = start + numRecords;
//
//            final int scheduledPodsRange = start + (int) (numRecords * (pendingPods / 100));
//
//            try {
//                for (int j = start; j < end; j++) {
//                    final String labelOperator =
//                            getLabelOperator(labels, ranges, ((double) (j - start)) / numRecords);
//                    final String node = "node" + (j % numNodes);
//                    final String pod = "pod" + j;
//
//                    final String taintKey = "taint_key" + (j % numNodes);
//                    final String taintValue = "taint_value" + (j % numNodes);
//
//                    final String labelKey = "label_key" + (j % numNodes);
//                    final String labelValue = "label_value" + (j % numNodes);
//
//                    final String antiAffinitylabelKey = "label_key" + (j % (numNodes - 1));
//                    final String antiAffinitylabelValue = "label_value" + (j % (numNodes - 1));
//
//                    final String nullString = "null";
//
//                    podStmt.setString(1, pod);
//                    podStmt.setString(2, (j > scheduledPodsRange ? "Scheduled" : "Pending"));
//                    podStmt.setString(3, (j > scheduledPodsRange ? node : nullString));
//
//                    podPortsRequestStmt.setString(1, pod);
//
//                    containerHostPortsStmt.setString(1, pod);
//                    containerHostPortsStmt.setString(2, (j > scheduledPodsRange ? node : nullString));
//
//                    podNodeSelectorLabelsStmt.setString(1, pod);
//                    podNodeSelectorLabelsStmt.setString(2, labelKey);
//                    podNodeSelectorLabelsStmt.setString(3, labelOperator);
//                    podNodeSelectorLabelsStmt.setString(4, labelValue);
//
//                    podsAffinityMatchExpressionsStmt.setString(1, pod);
//                    podsAffinityMatchExpressionsStmt.setString(2, labelKey);
//                    podsAffinityMatchExpressionsStmt.setString(3, labelOperator);
//                    podsAffinityMatchExpressionsStmt.setString(4, labelValue);
//
//                    podsAntiAffinityMatchExpressionsStmt.setString(1, pod);
//                    podsAntiAffinityMatchExpressionsStmt.setString(2, antiAffinitylabelKey);
//                    podsAntiAffinityMatchExpressionsStmt.setString(3, labelOperator);
//                    podsAntiAffinityMatchExpressionsStmt.setString(4, antiAffinitylabelValue);
//
//
//                    podLabelsStmt.setString(1, pod);
//                    podLabelsStmt.setString(2, labelKey);
//                    podLabelsStmt.setString(3, labelValue);
//
//                    volumeLabelsStmt.setString(1, pod);
//                    volumeLabelsStmt.setString(2, labelKey);
//                    volumeLabelsStmt.setString(3, labelValue);
//
//                    podByServiceStmt.setString(1, pod);
//
//                    podTolerationsStmt.setString(1, pod);
//                    podTolerationsStmt.setString(2, taintKey);
//                    podTolerationsStmt.setString(3, taintValue);
//                    podTolerationsStmt.setString(4, labelOperator);
//
//                    podImagesStmt.setString(1, pod);
//
//                    podStmt.executeUpdate();
//                    podNodeSelectorLabelsStmt.executeUpdate();
//                    podPortsRequestStmt.executeUpdate();
//                    podsAffinityMatchExpressionsStmt.executeUpdate();
//                    podsAntiAffinityMatchExpressionsStmt.executeUpdate();
//                    podLabelsStmt.executeUpdate();
//                    volumeLabelsStmt.executeUpdate();
//                    podByServiceStmt.executeUpdate();
//                    podTolerationsStmt.executeUpdate();
//                    podImagesStmt.executeUpdate();
//
//                }
//                System.out.println("Finished insertions to SQL");
//                getResults();
//                if (exerciseDeletesAndUpdates) {
//                    // remove the last set
//                    dbCtx.execute("delete from pod where status = 'Scheduled'");
//                    getResults();
//                    // this should only affect sparecapacity
//                    dbCtx.execute("update pod set status = 'Scheduled', node_name = 'node1' " +
//                            "where status = 'Pending'");
//                    getResults();
//                }
//                System.out.println("Finished insertions to DDlog");
//            } catch (final SQLException e) {
//                System.out.println(Arrays.toString(e.getStackTrace()));
//                throw new RuntimeException(e);
//            }
//        }
//    }
//
//    /*
//     * Tears down connection to db.
//     */
//    @TearDown(Level.Invocation)
//    public void teardown() throws SQLException {
//        if (useDDlog) {
//            updater.close();
//        } else {
//            dbCtx.close();
//        }
//    }
//
//    private void printSQLResult(final String viewName) {
//        final Result<? extends Record> results =
//                dbCtx.resultQuery("select * from " + viewName).fetch();
//        System.out.println(String.format("%s: rows: %d", viewName, results.size()));
//    }
//
//    private void getResults() {
//        if (useDDlog) {
//            updater.flushUpdates();
//        } else {
//            for (final String viewName: views) {
//                printSQLResult(viewName);
//            }
//        }
//    }
//
//    private Model buildModel(final DSLContext dslCtx, final List<String> views, final String testName) {
//        // get model file for the current test
//        final File modelFile = new File("resources/" + testName + ".mzn");
//        // create data file
//        final File dataFile = new File("/tmp/" + testName + ".dzn");
//        return Model.buildModel(dslCtx, views, modelFile, dataFile);
//    }

}