/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.base.Splitter;

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
import java.util.Set;
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
public class DBViews {
    private static final String UNFIXED_PODS_VIEW_NAME = "pods_to_assign";
    private static final String FIXED_PODS_VIEW_NAME = "assigned_pods";
    private static final String INITIAL_PLACEMENT_VIEW_NAME_SUFFIX = "";
    static final String PREEMPTION_VIEW_NAME_SUFFIX = "_preempt";
    static final String SCOPE_VIEW_NAME_SUFFIX = "_augment";
    static final String SORT_VIEW_NAME_SUFFIX = "_sorted";
    private static final ViewStatements INITIAL_PLACEMENT = new ViewStatements(INITIAL_PLACEMENT_VIEW_NAME_SUFFIX);
    private static final ViewStatements PREEMPTION = new ViewStatements(PREEMPTION_VIEW_NAME_SUFFIX);
    private static final Set<String> INITIAL_PLACEMENT_VIEW_NAMES;

    static {
        allPendingPods(INITIAL_PLACEMENT);
        initialPlacementInputPods(INITIAL_PLACEMENT);
        initialPlacementFixedPods(INITIAL_PLACEMENT);
        preemptionInputPods(PREEMPTION);
        preemptionFixedPods(PREEMPTION);
        Stream.of(INITIAL_PLACEMENT, PREEMPTION).forEach(viewStatements -> {
            matchingNodes(viewStatements);
            matchingPods(viewStatements);
            podsWithPortRequests(viewStatements);
            podNodeSelectorMatches(viewStatements);
            spareCapacityPerNode(viewStatements);
            podsThatTolerateNodeTaints(viewStatements);
            nodesThatHaveTolerations(viewStatements);
            allowedNodes(viewStatements);
            interPodAffinityAndAntiAffinity(viewStatements);
            topologyKeyChannels(viewStatements);
            podTopologySpread(viewStatements);
        });
        INITIAL_PLACEMENT_VIEW_NAMES = INITIAL_PLACEMENT.asQuery.keySet();
    }

    static Set<String> initialPlacementViewNames() {
        return INITIAL_PLACEMENT_VIEW_NAMES;
    }

    static List<String> getSchema() {
        final List<String> schema = new ArrayList<>();
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/scheduler_tables.sql");
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> baseTableList = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);
            schema.addAll(baseTableList);
            schema.addAll(INITIAL_PLACEMENT.getViewStatements());
            schema.addAll(PREEMPTION.getViewStatements());
            return schema;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Select all pods that are pending placement.
     */
    private static void allPendingPods(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT";
        final String query = """
                              SELECT pod_info.*, node_name AS controllable__node_name
                              FROM pod_info
                              WHERE status = 'Pending'
                                AND node_name IS NULL
                                AND schedulerName = 'dcm-scheduler'
                                AND LAST_REQUEUE + 1000 <=
                                    (SELECT 1000 * extract(epoch FROM current_timestamp())) -- timestamp in ms
                             """;
        viewStatements.addQuery(name, query);
    }

    /*
     * Select 50 pods that need to be considered for initial placement.
     */
    private static void initialPlacementInputPods(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN";
        final String query = "SELECT * FROM PODS_TO_ASSIGN_NO_LIMIT LIMIT 50";
        viewStatements.addQuery(name, query);
    }

    /*
     * Pods that are already assigned to nodes
     */
    private static void initialPlacementFixedPods(final ViewStatements viewStatements) {
        final String name = "ASSIGNED_PODS";
        final String query = "SELECT * FROM pod_info WHERE node_name IS NOT NULL";
        viewStatements.addQuery(name, query);
    }

    /*
     * Select all pods that need to be scheduled and pods of comparatively lower priority that could be reassigned.
     */
    private static void preemptionInputPods(final ViewStatements viewStatements) {
        final String name = "PODS_TO_ASSIGN";
        final String query = """
                             (SELECT * FROM PODS_TO_ASSIGN)
                             UNION
                             (SELECT *, node_name AS controllable__node_name FROM pod_info
                              WHERE node_name IS NOT NULL AND priority < (SELECT MAX(priority) FROM PODS_TO_ASSIGN))""";
        viewStatements.addQuery(name, query);
    }

    /*
     * Fix all pods that have higher priority than the set of pods pending pods.
     */
    private static void preemptionFixedPods(final ViewStatements viewStatements) {
        final String name = "ASSIGNED_PODS";
        final String query = """
                            SELECT *, node_name AS controllable__node_name FROM pod_info
                            WHERE node_name IS NOT NULL AND priority >= (SELECT MAX(priority) FROM PODS_TO_ASSIGN)""";
        viewStatements.addQuery(name, query);
    }

    /*
     * Pods with port requests
     */
    private static void podsWithPortRequests(final ViewStatements viewStatements) {
        final String name = "PODS_WITH_PORT_REQUESTS";
        final String baseQuery = """
                SELECT pods_to_assign.uid as pod_uid,
                       ARRAY_AGG(other_pods.uid) OVER (PARTITION BY pods_to_assign.uid) AS pod_matches,
                       ARRAY_AGG(other_pods.node_name) OVER (PARTITION BY pods_to_assign.uid) AS node_matches
                FROM $pendingPods pods_to_assign
                JOIN pod_ports_request ppr1 ON pods_to_assign.uid = ppr1.pod_uid
                JOIN pod_ports_request ppr2
                    ON pods_to_assign.uid != ppr2.pod_uid
                    AND ppr1.host_port = ppr2.host_port
                    AND ppr1.host_protocol = ppr2.host_protocol
                    AND (ppr1.host_ip = ppr2.host_ip
                         OR ppr1.host_ip = '0.0.0.0'
                         OR ppr2.host_ip = '0.0.0.0')
                JOIN $otherPods other_pods
                    ON other_pods.uid = ppr2.pod_uid"""
                .replace("$pendingPods", viewStatements.unfixedPods);
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
                             SELECT DISTINCT expr_id, node_name
                             FROM match_expressions me
                             JOIN node_labels
                                  ON (me.label_operator = 'In'
                                     AND me.label_key = node_labels.label_key
                                     AND array_contains(me.label_values, node_labels.label_value))
                                   OR (me.label_operator = 'Exists'
                                     AND me.label_key = node_labels.label_key)
                                   OR (me.label_operator = 'NotIn')
                                   OR (me.label_operator = 'DoesNotExist')
                             GROUP BY expr_id, label_operator, node_name
                             HAVING CASE me.label_operator
                                       WHEN 'NotIn'
                                           THEN NOT(ANY(me.label_key = node_labels.label_key
                                                     AND array_contains(me.label_values, node_labels.label_value)))
                                       WHEN 'DoesNotExist'
                                           THEN NOT(ANY(me.label_key = node_labels.label_key))
                                       ELSE 1 = 1
                                    END""";
        viewStatements.addQuery(name, query);
    }

    /*
     * For each match expression, find the set of pods that match it
     */
    private static void matchingPods(final ViewStatements viewStatements) {
        final String name = "MATCHING_PODS";
        final String query = """
                            SELECT expr_id, label_operator, pod_uid
                            FROM match_expressions me
                            JOIN pod_labels
                                 ON (me.label_operator = 'In'
                                    AND me.label_key = pod_labels.label_key
                                    AND array_contains(me.label_values, pod_labels.label_value))
                                  OR (me.label_operator = 'Exists'
                                    AND me.label_key = pod_labels.label_key)
                                  OR (me.label_operator = 'NotIn')
                                  OR (me.label_operator = 'DoesNotExist')
                            GROUP BY expr_id, label_operator, pod_uid
                            HAVING CASE me.label_operator
                                      WHEN 'NotIn'
                                          THEN NOT(ANY(me.label_key = pod_labels.label_key
                                                    AND array_contains(me.label_values, pod_labels.label_value)))
                                      WHEN 'DoesNotExist'
                                          THEN NOT(ANY(me.label_key = pod_labels.label_key))
                                      ELSE 1 = 1
                                   END""";
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
        final String query = """
                    SELECT pods_to_assign.uid AS pod_uid,
                           array_agg(distinct matching_nodes.node_name) over (partition by pod_uid) node_matches
                    FROM $pendingPods AS pods_to_assign
                    JOIN pod_node_selector_labels pnsl
                         ON pods_to_assign.uid = pnsl.pod_uid
                    JOIN matching_nodes
                         ON array_contains(pnsl.match_expressions, matching_nodes.expr_id)
                    WHERE pods_to_assign.has_node_selector_labels = true
                    GROUP BY pods_to_assign.uid, pnsl.match_expressions, pnsl.term, matching_nodes.node_name
                    HAVING array_length(pnsl.match_expressions) = COUNT(DISTINCT matching_nodes.expr_id)
                    """.replace("$pendingPods", viewStatements.unfixedPods);
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
                       allocatable - CAST(sum(ISNULL(A.demand, 0)) as bigint) as capacity
                FROM node_info
                JOIN node_resources
                    ON node_info.uid = node_resources.uid
                LEFT JOIN (SELECT pod_info.node_name,
                             pod_resource_demands.resource,
                             pod_resource_demands.demand
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
                
                UNION
                
                -- For every resource X being requested that are not available on any node,
                -- generate a row for each node with resource X and with 0 capacity
                SELECT node_info.name, p.resource, 0
                FROM node_info
                CROSS JOIN (SELECT distinct resource
                            FROM $pendingPods pods_to_assign
                            NATURAL JOIN pod_resource_demands) p
                JOIN node_resources ON node_info.uid = node_resources.uid
                GROUP BY node_info.name, p.resource
                HAVING NOT ANY(p.resource = node_resources.resource)
                """.replace("$pendingPods", viewStatements.unfixedPods);
        viewStatements.addQuery(name, query);
    }

    /*
     * For each pod, extract nodes that match according to taints/tolerations.
     */
    private static void podsThatTolerateNodeTaints(final ViewStatements viewStatements) {
        final String name = "PODS_THAT_TOLERATE_NODE_TAINTS";
        final String query = """
                             SELECT pods_to_assign.uid AS pod_uid,
                                    A.node_name AS node_name
                             FROM $pendingPods AS pods_to_assign
                             JOIN pod_tolerations
                                  ON pods_to_assign.uid = pod_tolerations.pod_uid
                             JOIN (SELECT *, COUNT(*) OVER (PARTITION BY node_name) AS num_taints
                                   FROM node_taints) AS A
                                  ON pod_tolerations.tolerations_key = A.taint_key
                                  AND (pod_tolerations.tolerations_effect = null
                                       OR pod_tolerations.tolerations_effect = A.taint_effect)
                                  AND (pod_tolerations.tolerations_operator = 'Exists'
                                       OR pod_tolerations.tolerations_value = A.taint_value)
                             GROUP BY pod_tolerations.pod_uid, A.node_name, A.num_taints
                             HAVING COUNT(*) = A.num_taints
                             """.replace("$pendingPods", viewStatements.unfixedPods);
        viewStatements.addQuery(name, query);
    }

    /*
     * The set of nodes that have any tolerations configured
     */
    private static void nodesThatHaveTolerations(final ViewStatements viewStatements) {
        final String name = "NODES_THAT_HAVE_TOLERATIONS";
        final String query = "SELECT distinct node_name FROM node_taints";
        viewStatements.addQuery(name, query);
    }

    /*
     * Avoid overloaded nodes or nodes that report being under resource pressure
     */
    private static void allowedNodes(final ViewStatements viewStatements) {
        final String name = "ALLOWED_NODES";
        final String query = "SELECT name FROM spare_capacity_per_node;";
        viewStatements.addQuery(name, query);
    }

    /*
     * Inter-pod affinity and anti-affinity
     */
    private static void interPodAffinityAndAntiAffinity(final ViewStatements viewStatements) {
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
        final String formatString = """
                        SELECT DISTINCT
                            pods_to_assign.uid as pod_uid,
                            ARRAY_AGG(matching_pods.pod_uid) OVER (PARTITION BY pods_to_assign.uid) AS pod_matches,
                            ARRAY_AGG(other_pods.node_name) OVER (PARTITION BY pods_to_assign.uid) AS node_matches
                        FROM $pendingPods AS pods_to_assign
                        JOIN pod_$affinityType_match_expressions AS match_expressions ON
                             pods_to_assign.uid = match_expressions.pod_uid
                        JOIN matching_pods
                            ON array_contains(match_expressions.match_expressions, matching_pods.expr_id)
                        JOIN $otherPods as other_pods
                            ON matching_pods.pod_uid = other_pods.uid AND pods_to_assign.uid != other_pods.uid
                        WHERE pods_to_assign.has_pod_$affinityType_requirements = true
                        GROUP BY
                            pods_to_assign.uid,
                            matching_pods.pod_uid,
                            label_selector,
                            topology_key,
                            match_expressions,
                            other_pods.node_name
                        HAVING array_length(match_expressions) = COUNT(DISTINCT matching_pods.expr_id)
                    """;
        for (final String type: List.of("affinity", "anti_affinity")) {
            final String baseQuery = formatString.replace("$affinityType",  type)
                                                 .replace("$pendingPods", viewStatements.unfixedPods);
            final String pendingQuery = baseQuery.replace("$otherPods", viewStatements.unfixedPods);
            final String scheduledQuery = baseQuery.replace("$otherPods", viewStatements.fixedPods);
            viewStatements.addQuery(String.format("INTER_POD_%s_MATCHES_PENDING", type.toUpperCase(Locale.ROOT)),
                             pendingQuery);
            viewStatements.addQuery(String.format("INTER_POD_%s_MATCHES_SCHEDULED", type.toUpperCase(Locale.ROOT)),
                             scheduledQuery);
        }
    }

    private static void topologyKeyChannels(final ViewStatements viewStatements) {
        final String queryPending = """
                        SELECT DISTINCT
                            pod_topology_spread_constraints.match_expressions AS group_name,
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
                    SELECT DISTINCT
                           ptsc.match_expressions AS group_name,
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
            if (asQuery.containsKey(name)) {
                throw new RuntimeException("Duplicate view " + name + " " + query);
            }
            asQuery.put(name + viewNameSuffix, query);
        }

        List<String> getViewStatements() {
            return asQuery.entrySet().stream()
                          .map(e -> String.format("%nCREATE VIEW %s AS %s%n", e.getKey(), e.getValue()))
                          .collect(Collectors.toList());
        }
    }
}
