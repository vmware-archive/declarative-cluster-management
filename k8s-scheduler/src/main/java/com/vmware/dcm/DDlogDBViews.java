/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.base.Splitter;
import com.vmware.ddlog.DDlogJooqProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * This represents views/queries executed in the database that supply inputs to constraints in DCM models
 * (see {@link Policies}). We have two sets of queries, one for initial placement and one for preemption.
 * Both of these views consider different groups of a) Unfixed pods: pods that can be assigned/reassigned,
 * b) Fixed pods: pods that cannot be reassigned.
 *
 * SQL does allow parameterizing the tables being queried. We therefore resort to dynamic SQL to create two
 * sets of views, each parameterized by a different pair of "fixed" and "unfixed" pods.
 *
 * The {@link Scheduler} queries views for a given DCM model by adding a suffix to all view names being
 * queried.
 */
public class DDlogDBViews {
    private static final String UNFIXED_PODS_VIEW_NAME = "pods_to_assign";
    private static final String FIXED_PODS_VIEW_NAME = "assigned_pods";
    private static final String INITIAL_PLACEMENT_VIEW_NAME_SUFFIX = "";
    static final String PREEMPTION_VIEW_NAME_SUFFIX = "_preempt";
    private static final ViewStatements INITIAL_PLACEMENT = new ViewStatements(INITIAL_PLACEMENT_VIEW_NAME_SUFFIX);
    private static final ViewStatements PREEMPTION = new ViewStatements(PREEMPTION_VIEW_NAME_SUFFIX);

    static {
        allPendingPodsNoRequeue(INITIAL_PLACEMENT);
        allPendingPodsWithRequeue(INITIAL_PLACEMENT);
        allPendingPods(INITIAL_PLACEMENT);
        initialPlacementInputPods(INITIAL_PLACEMENT);
        initialPlacementFixedPods(INITIAL_PLACEMENT);
        //helperView(PREEMPTION);
        //preemptionInputPods(PREEMPTION);
        //preemptionFixedPods(PREEMPTION);
        Stream.of(INITIAL_PLACEMENT).forEach(viewStatements -> {
            matchingNodes(viewStatements);
            matchingPods(viewStatements);
            podsWithPortRequests(viewStatements);
            podNodeSelectorMatches(viewStatements);
            spareCapacityPerNode(viewStatements);
            podsThatTolerateNodeTaints(viewStatements);
            nodesThatHaveTolerations(viewStatements);
            allowedNodes(viewStatements);
            interPodAffinityAndAntiAffinitySimple(viewStatements);
            getCount(viewStatements);
            //topologyKeyChannels(viewStatements);
            //podTopologySpread(viewStatements);
        });
    }

    static List<String> getSchema() {
        final List<String> schema = new ArrayList<>();
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/ddlog_scheduler_tables.sql");
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> baseTableList = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);

            // Now create an identity view per table, so that we can query input relations (by querying the output view)
            // in the DDlog backend.
            final Pattern getTableName = Pattern.compile("create table (.+)");

            final List<String> identityViews = baseTableList.stream().filter(s -> s.startsWith("create table"))
                    .map(s -> {
                        final Matcher m = getTableName.matcher(s);
                        if (m.find()) {
                            final String tmp = s.substring("create table ".length(), m.end());
                            return String.format(
                                    "create view %s as select distinct * from %s",
                                    DDlogJooqProvider.toIdentityViewName(tmp), tmp);
                        } else {
                            throw new RuntimeException("help");
                        }
                    }).collect(Collectors.toList());

            schema.addAll(baseTableList);
            schema.addAll(identityViews);
            schema.addAll(INITIAL_PLACEMENT.getViewStatements());
            schema.addAll(PREEMPTION.getViewStatements());
            return schema;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void allPendingPodsNoRequeue(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT_NO_REQUEUE";
        final String query = """
                             SELECT DISTINCT
                               pod_info.uid, pod_info.pod_name,
                               pod_info.status,
                               pod_info.node_name,
                               pod_info.namespace,
                               pod_info.owner_name,
                               pod_info.creation_timestamp,
                               pod_info.priority,
                               pod_info.scheduler_name,
                               pod_info.has_node_selector_labels,
                               pod_info.has_pod_affinity_requirements,
                               pod_info.has_pod_anti_affinity_requirements,
                               pod_info.has_node_port_requirements,
                               pod_info.has_topology_spread_constraints,
                               pod_info.equivalence_class,
                               pod_info.qos_class,
                               pod_info.resourceversion,
                               pod_info.last_requeue,
                               pod_info.node_name as controllable__node_name
                             FROM pod_info
                             WHERE last_requeue = CAST(0 as bigint)
                                 AND status = 'Pending'
                                 AND node_name IS NULL
                                 AND scheduler_name = 'dcm-scheduler'
                             """;
        viewStatements.addQuery(name, query);
    }

    private static void allPendingPodsWithRequeue(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT_WITH_REQUEUE";
        final String query = """
                             SELECT DISTINCT
                               pod_info.uid, pod_info.pod_name,
                               pod_info.status,
                               pod_info.node_name,
                               pod_info.namespace,
                               pod_info.owner_name,
                               pod_info.creation_timestamp,
                               pod_info.priority,
                               pod_info.scheduler_name,
                               pod_info.has_node_selector_labels,
                               pod_info.has_pod_affinity_requirements,
                               pod_info.has_pod_anti_affinity_requirements,
                               pod_info.has_node_port_requirements,
                               pod_info.has_topology_spread_constraints,
                               pod_info.equivalence_class,
                               pod_info.qos_class,
                               pod_info.resourceversion,
                               pod_info.last_requeue,
                               pod_info.node_name as controllable__node_name 
                             FROM pod_info
                             WHERE last_requeue != CAST(0 as bigint)
                                 AND status = 'Pending'
                                 AND node_name IS NULL 
                                 AND scheduler_name = 'dcm-scheduler'
                             """;
        viewStatements.addQuery(name, query);
    }

    /*
     * Select all pods that are pending placement.
     */
    private static void allPendingPods(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT";
        final String query = """
                             SELECT DISTINCT * FROM pods_to_assign_no_limit_no_requeue
                             UNION
                             SELECT DISTINCT
                               pods_to_assign_no_limit_with_requeue.uid, pods_to_assign_no_limit_with_requeue.pod_name,
                               pods_to_assign_no_limit_with_requeue.status,
                               pods_to_assign_no_limit_with_requeue.node_name,
                               pods_to_assign_no_limit_with_requeue.namespace,
                               pods_to_assign_no_limit_with_requeue.owner_name,
                               pods_to_assign_no_limit_with_requeue.creation_timestamp,
                               pods_to_assign_no_limit_with_requeue.priority,
                               pods_to_assign_no_limit_with_requeue.scheduler_name,
                               pods_to_assign_no_limit_with_requeue.has_node_selector_labels,
                               pods_to_assign_no_limit_with_requeue.has_pod_affinity_requirements,
                               pods_to_assign_no_limit_with_requeue.has_pod_anti_affinity_requirements,
                               pods_to_assign_no_limit_with_requeue.has_node_port_requirements,
                               pods_to_assign_no_limit_with_requeue.has_topology_spread_constraints,
                               pods_to_assign_no_limit_with_requeue.equivalence_class,
                               pods_to_assign_no_limit_with_requeue.qos_class,
                               pods_to_assign_no_limit_with_requeue.resourceversion,
                               pods_to_assign_no_limit_with_requeue.last_requeue,
                               pods_to_assign_no_limit_with_requeue.controllable__node_name
                              FROM pods_to_assign_no_limit_with_requeue
                             JOIN timer_t
                             ON pods_to_assign_no_limit_with_requeue.last_requeue < timer_t.tick
                             """;
        viewStatements.addQuery(name, query);
    }

    private static void getCount(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT_COUNT";
        final String query = "SELECT COUNT(*) from PODS_TO_ASSIGN_NO_LIMIT";
        viewStatements.addQuery(name, query);
    }

    /*
     * Select 50 pods that need to be considered for initial placement.
     */
    private static void initialPlacementInputPods(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN";
        final String query = "SELECT DISTINCT * FROM PODS_TO_ASSIGN_NO_LIMIT LIMIT 50";
        viewStatements.addQuery(name, query);
    }

    /*
     * Pods that are already assigned to nodes
     */
    private static void initialPlacementFixedPods(final ViewStatements viewStatements) {
        final String name = "ASSIGNED_PODS";
        final String query = "SELECT DISTINCT * FROM pod_info WHERE node_name IS NOT NULL";
        viewStatements.addQuery(name, query);
    }

    private static void helperView(final ViewStatements viewStatements) {
        final String name = "HELPER_VIEW";
        final String query = "SELECT DISTINCT node_name," +
                " MAX(pods_to_assign.priority) as m " +
                " FROM pods_to_assign" +
                " GROUP BY node_name";

        viewStatements.addQuery(name, query);
    }


    /*
     * Select all pods that need to be scheduled and pods of comparatively lower priority that could be reassigned.
     */
    private static void preemptionInputPods(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN";
        final String query = """
                            (SELECT DISTINCT * FROM PODS_TO_ASSIGN)
                            UNION
                            (SELECT DISTINCT pod_info.uid, pod_info.pod_name,
                            pod_info.status,
                            pod_info.node_name,
                            pod_info.namespace,
                            pod_info.owner_name,
                            pod_info.creation_timestamp,
                            pod_info.priority,
                            pod_info.scheduler_name,
                            pod_info.has_node_selector_labels,
                            pod_info.has_pod_affinity_requirements,
                            pod_info.has_pod_anti_affinity_requirements,
                            pod_info.has_node_port_requirements,
                            pod_info.has_topology_spread_constraints,
                            pod_info.equivalence_class,
                            pod_info.qos_class,
                            pod_info.resourceversion,
                            pod_info.last_requeue,
                            pod_info.node_name as controllable__node_name
                             FROM pod_info JOIN helper_view_preempt ON
                             pod_info.priority < helper_view_preempt.m
                             WHERE pod_info.node_name IS NOT NULL
                            )""";
        viewStatements.addQuery(name, query);
    }

    /*
     * Fix all pods that have higher priority than the set of pods pending pods.
     */
    private static void preemptionFixedPods(final ViewStatements viewStatements) {
        final String name = "ASSIGNED_PODS";
        final String query = """
                        SELECT DISTINCT pod_info.uid, pod_info.pod_name,
                        pod_info.status,
                        pod_info.node_name,
                        pod_info.namespace,
                        pod_info.owner_name,
                        pod_info.creation_timestamp,
                        pod_info.priority,
                        pod_info.scheduler_name,
                        pod_info.has_node_selector_labels,
                        pod_info.has_pod_affinity_requirements,
                        pod_info.has_pod_anti_affinity_requirements,
                        pod_info.has_node_port_requirements,
                        pod_info.has_topology_spread_constraints,
                        pod_info.equivalence_class,
                        pod_info.qos_class,
                        pod_info.resourceversion,
                        pod_info.last_requeue,
                        pod_info.node_name as controllable__node_name
                         FROM pod_info JOIN helper_view_preempt ON
                         pod_info.priority >= helper_view_preempt.m
                         WHERE pod_info.node_name IS NOT NULL
                        """;
        viewStatements.addQuery(name, query);
    }

    /*
     * Pods with port requests
     */
    private static void podsWithPortRequests(final ViewStatements viewStatements) {
        final String name = "PODS_WITH_PORT_REQUESTS";
        String baseQuery = "SELECT DISTINCT pods_to_assign.uid as pod_uid, \n" +
                "          ARRAY_AGG(other_pods.uid) OVER (PARTITION BY pods_to_assign.uid) AS pod_matches, \n" +
                "          ARRAY_AGG(other_pods.node_name) OVER (PARTITION BY pods_to_assign.uid) AS node_matches  \n" +
                "     FROM $herp AS pods_to_assign\n" +
                "     JOIN pod_ports_request AS ppr1 ON pods_to_assign.uid = ppr1.pod_uid\n" +
                "     JOIN pod_ports_request AS ppr2\n" +
                "         ON pods_to_assign.uid != ppr2.pod_uid\n" +
                "         AND ppr1.host_port = ppr2.host_port\n" +
                "         AND ppr1.host_protocol = ppr2.host_protocol\n" +
                "         AND (ppr1.host_ip = ppr2.host_ip\n" +
                "              OR ppr1.host_ip = '0.0.0.0'\n" +
                "              OR ppr2.host_ip = '0.0.0.0')\n" +
                "     JOIN $otherPods AS other_pods\n" +
                "         ON other_pods.uid = ppr2.pod_uid";
        baseQuery = baseQuery.replace("$herp", viewStatements.unfixedPods);
        final String pendingQuery = baseQuery.replace("$otherPods", viewStatements.unfixedPods);
        final String scheduledQuery = baseQuery.replace("$otherPods", viewStatements.fixedPods);
        viewStatements.addQuery(name + "_PENDING", pendingQuery);
        viewStatements.addQuery(name + "_SCHEDULED", scheduledQuery);
    }

    /*
     * For each match expression, find the set of nodes that match it
     */
    private static void matchingNodes(final ViewStatements viewStatements) {
        final String name = "MATCHING_NODES";
        final String query = """
                    (SELECT DISTINCT expr_id, node_name
                     FROM (SELECT DISTINCT expr_id, label_key, label_value
                           FROM match_expressions
                           WHERE match_expressions.label_operator = 'In') me
                     JOIN node_labels
                        ON me.label_key = node_labels.label_key
                           AND me.label_value = node_labels.label_value)
                    UNION
                    
                    (SELECT DISTINCT expr_id, node_name
                    FROM (SELECT DISTINCT expr_id, label_key
                           FROM match_expressions
                           WHERE match_expressions.label_operator = 'Exists') me
                    JOIN node_labels
                        ON me.label_key = node_labels.label_key)
                    """;
        viewStatements.addQuery(name, query);
    }

    /*
     * For each match expression, find the set of pods that match it
     */
    private static void matchingPods(final ViewStatements viewStatements) {
        final String name = "MATCHING_PODS";
        final String query = """
                    (SELECT DISTINCT expr_id, pod_uid
                     FROM (SELECT DISTINCT expr_id, label_key, label_value
                           FROM match_expressions
                           WHERE match_expressions.label_operator = 'In') me
                     JOIN pod_labels
                        ON me.label_key = pod_labels.label_key
                           AND me.label_value = pod_labels.label_value)
                    UNION
                    
                    (SELECT DISTINCT expr_id, pod_uid
                    FROM (SELECT DISTINCT expr_id, label_key
                           FROM match_expressions
                           WHERE match_expressions.label_operator = 'Exists') me
                    JOIN pod_labels
                        ON me.label_key = pod_labels.label_key)
                    """;
        viewStatements.addQuery(name, query);
    }

    /*
     * For each pod, get the nodes that match its node selector labels.
     */
    private static void podNodeSelectorMatches(final ViewStatements viewStatements) {
        //
        // https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
        //
        // Multiple match expressions within a nodeSelectorTerm is treated as a logical AND. This
        // is accounted for in the GROUP BY + HAVING clause below. That is, we make sure that for a node to be counted,
        // it has to match all match_expression IDs in the pod_node_selector_labels.match_expression array.
        //
        // Multiple nodeSelectorTerms (pod_node_selector_labels.term) are treated as a logical OR.
        // We can therefore array_agg() all nodes over multiple such terms and only need to group by
        // pod.uid below.
        //
        final String name = "POD_NODE_SELECTOR_MATCHES";
        final String query = String.format(
                    "SELECT DISTINCT pods_to_assign.uid AS pod_uid, " +
                    "       array_agg( matching_nodes.node_name) over (partition by pods_to_assign.uid) node_matches " +
                    "FROM %s AS pods_to_assign " +
                    "JOIN pod_node_selector_labels pnsl" +
                    "     ON pods_to_assign.uid = pnsl.pod_uid " +
                    "JOIN matching_nodes " +
                    "     ON array_contains(pnsl.match_expressions, matching_nodes.expr_id) " +
                    "WHERE pods_to_assign.has_node_selector_labels = true " +
                   "GROUP BY pods_to_assign.uid, pnsl.match_expressions, pnsl.term, matching_nodes.node_name " +
                    "HAVING array_length(pnsl.match_expressions) = COUNT(DISTINCT matching_nodes.expr_id)",
                    viewStatements.unfixedPods);
        viewStatements.addQuery(name, query);
    }

    /*
     * Spare capacity per node
     */
    private static void spareCapacityPerNode(final ViewStatements viewStatements) {
        final String name = "SPARE_CAPACITY_PER_NODE";
        final String query = """
                -- For resources that are in use, compute the total spare capacity per node
                SELECT node_info.name AS name,
                       node_resources.resource,
                       node_resources.allocatable - CAST(sum(A.total_demand) as bigint) as capacity
                FROM node_info
                JOIN node_resources
                    ON node_info.uid = node_resources.uid
                LEFT JOIN (SELECT DISTINCT pod_info.node_name,
                             pod_resource_demands.resource,
                             pod_resource_demands.demand AS total_demand
                     FROM pod_info
                     JOIN pod_resource_demands
                       ON pod_resource_demands.uid = pod_info.uid) A
                    ON A.node_name = node_info.name AND A.resource = node_resources.resource
                WHERE unschedulable = false AND
                      memory_pressure = false AND
                      out_of_disk = false AND
                      disk_pressure = false AND
                      pid_pressure = false AND
                      network_unavailable = false AND
                      ready = true
                GROUP BY node_info.name, node_resources.resource, node_resources.allocatable
                """
                /*
                
                UNION                
                -- For every resource X being requested that are not available on any node,
                -- generate a row for each node with resource X and with 0 capacity
                (SELECT node_info.name, p.resource, p.zero as capacity
                FROM node_info NATURAL JOIN node_resources
                CROSS JOIN (SELECT distinct resource, CAST(0 as bigint) as zero
                            FROM $pendingPods pods_to_assign NATURAL JOIN pod_resource_demands) p
                GROUP BY node_info.name, p.resource, node_resources.resource, p.zero
                HAVING p.resource NOT IN (node_resources.resource))
                """*/
                .replace("$pendingPods", viewStatements.unfixedPods);
        viewStatements.addQuery(name, query);
    }

    /*
     * For each pod, extract nodes that match according to taints/tolerations.
     */
    private static void podsThatTolerateNodeTaints(final ViewStatements viewStatements) {
        final String name = "PODS_THAT_TOLERATE_NODE_TAINTS";
        final String query = String.format(
                             "SELECT DISTINCT pod_tolerations.pod_uid AS pod_uid, " +
                             "       A.node_name AS node_name " +
                             "FROM %s AS pods_to_assign " +
                             "JOIN pod_tolerations " +
                             "     ON pods_to_assign.uid = pod_tolerations.pod_uid " +
                             "JOIN (SELECT DISTINCT *, COUNT(*) OVER (PARTITION BY node_name) AS num_taints " +
                             "      FROM node_taints) AS A " +
                             "     ON pod_tolerations.tolerations_key = A.taint_key " +
                             "     AND ( pod_tolerations.tolerations_effect IS NULL " +
                             "          OR pod_tolerations.tolerations_effect = A.taint_effect) " +
                             "     AND (pod_tolerations.tolerations_operator = 'Exists' " +
                             "          OR pod_tolerations.tolerations_value = A.taint_value) " +
                             "GROUP BY pod_tolerations.pod_uid, A.node_name, A.num_taints " +
                             "HAVING COUNT(*) = A.num_taints ", viewStatements.unfixedPods);
        viewStatements.addQuery(name, query);
    }

    /*
     * The set of nodes that have any tolerations configured
     */
    private static void nodesThatHaveTolerations(final ViewStatements viewStatements) {
        final String name = "NODES_THAT_HAVE_TOLERATIONS";
        final String query = "SELECT DISTINCT node_name FROM node_taints";
        viewStatements.addQuery(name, query);
    }

    /*
     * Avoid overloaded nodes or nodes that report being under resource pressure
     */
    private static void allowedNodes(final ViewStatements viewStatements) {
        final String name = "ALLOWED_NODES";
        final String query = "SELECT DISTINCT name FROM spare_capacity_per_node";
        viewStatements.addQuery(name, query);
    }

    /*
     * Inter-pod affinity and anti-affinity
     */
    private static void interPodAffinityAndAntiAffinitySimple(final ViewStatements viewStatements) {
        // The below query is simpler than it looks.
        //
        // All it does is the following:
        //   * A = join the set of pending pods with their configured <affinity/anti-affinity> match-expressions
        //   * B = join A with the matched-pods view to get the set of pods that match the required expressions
        //   * C = join with the set of <pending/fixed> pods that satisfy the matched expressions
        //   * D = select for each pending pod, the set of pods and nodes they are <affine / anti-affine> to
        //
        // The format string parameterizes the <pending/fixed> pods and whether we are producing the
        // result for <affinity/anti-affinity>

        final String formatString =
                "SELECT DISTINCT" +
                "  pods_to_assign.uid as pod_uid, " +
                "  matching_pods.pod_uid AS pod_matches, " +
                "  other_pods.node_name AS node_matches " +
                "FROM " +
                "  %2$s AS pods_to_assign " +
                "  JOIN pod_%1$s_match_expressions ON " +
                "        pods_to_assign.uid " +
                        "= pod_%1$s_match_expressions.pod_uid " +
                "  JOIN matching_pods " +
                "     ON pod_%1$s_match_expressions.%1$s_match_expression = matching_pods.expr_id " +
                "  JOIN %3$s as other_pods ON " +
                "           matching_pods.pod_uid = other_pods.uid" +
                "  WHERE pods_to_assign.has_pod_%1$s_requirements = true AND pods_to_assign.uid != other_pods.uid " +
                "GROUP BY " +
                "  pods_to_assign.uid, " +
                "  matching_pods.pod_uid, " +
                "  label_selector, " +
                "  topology_key, " +
                "  %1$s_match_expressions, " +
                "  other_pods.node_name " +
                "HAVING array_length(%1$s_match_expressions) = COUNT(DISTINCT matching_pods.expr_id)";
        for (final String type: List.of("affinity", "anti_affinity")) {
            final String pendingQuery = String.format(formatString, type, viewStatements.unfixedPods,
                                                                          viewStatements.unfixedPods);
            final String scheduledQuery = String.format(formatString, type, viewStatements.unfixedPods,
                                                                            viewStatements.fixedPods);
            viewStatements.addQuery(String.format("INTER_POD_%s_MATCHES_PENDING", type.toUpperCase(Locale.ROOT)),
                             pendingQuery);
            viewStatements.addQuery(String.format("INTER_POD_%s_MATCHES_SCHEDULED", type.toUpperCase(Locale.ROOT)),
                             scheduledQuery);
        }
    }

    private static void topologyKeyChannels(final ViewStatements viewStatements) {
        final String queryPending = """
                        SELECT DISTINCT
                            GROUP_CONCAT(distinct pod_topology_spread_constraints.match_expressions) AS group_name,
                            pods_to_assign.uid as pod_uid,
                            node_labels.node_name,
                            pod_topology_spread_constraints.topology_key,
                            'topology_value_variable' AS controllable__topology_value,
                            node_labels.label_value,
                            pod_topology_spread_constraints.max_skew,
                            ARRAY_AGG(node_labels.label_value)
                                OVER (PARTITION BY pod_topology_spread_constraints.match_expressions,
                                                   pod_topology_spread_constraints.topology_key) AS domain
                        FROM $pendingPods AS pods_to_assign
                        JOIN pod_topology_spread_constraints
                            ON pod_topology_spread_constraints.uid = pods_to_assign.uid
                        JOIN node_labels
                            ON pod_topology_spread_constraints.topology_key = node_labels.label_key
                        GROUP BY pod_topology_spread_constraints.match_expressions,
                                 pods_to_assign.uid,
                                 node_labels.node_name,
                                 pod_topology_spread_constraints.topology_key,
                                 node_labels.label_value
                """.replace("$pendingPods", viewStatements.unfixedPods);
        viewStatements.addQuery("POD_TO_TOPOLOGY_KEYS_PENDING", queryPending);
    }

    private static void podTopologySpread(final ViewStatements viewStatements) {
        final String query = """
                    SELECT GROUP_CONCAT(distinct ptsc.match_expressions) AS group_name,
                           nl.node_name,
                           nl.label_key,
                           nl.label_value,
                           ptsc.topology_key,
                           ptsc.max_skew,
                           COUNT(ptsc_outer.uid) AS total_demand
                    FROM $fixedPods pi
                    JOIN node_labels nl
                        ON nl.node_name = pi.node_name
                    JOIN pod_topology_spread_constraints ptsc
                        ON ptsc.topology_key = nl.label_key
                    LEFT JOIN pod_topology_spread_constraints ptsc_outer
                        ON ptsc_outer.topology_key = nl.label_key
                        AND pi.uid = ptsc_outer.uid
                    GROUP BY ptsc.match_expressions, nl.node_name, nl.label_key, nl.label_value
                """.replace("$fixedPods", viewStatements.fixedPods);
        viewStatements.addQuery("POD_TOPOLOGY_SPREAD_BOUNDS", query);
    }

    private static class ViewStatements {
        private final String unfixedPods;
        private final String fixedPods;
        private final String viewNameSuffix;
        private final Map<String, String> asQuery = new LinkedHashMap<>();

        ViewStatements(final String suffix) {
            this.unfixedPods = UNFIXED_PODS_VIEW_NAME + suffix;
            this.fixedPods = FIXED_PODS_VIEW_NAME + suffix;
            this.viewNameSuffix = suffix;
        }

        void addQuery(final String name, final String query) {
            asQuery.put(name + viewNameSuffix, query);
        }

        List<String> getViewStatements() {
            return asQuery.entrySet().stream()
                          .map(e -> String.format("%nCREATE VIEW %s AS %s%n", e.getKey(), e.getValue()))
                          .collect(Collectors.toList());
        }
    }
}