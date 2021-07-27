/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.jooq.CreateViewFinalStep;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class DBViews {
    private final DSLContext conn;
    private final Map<String, Supplier<CreateViewFinalStep>> asView;
    private final Map<String, Supplier<Result<Record>>> asQuery;

    public DBViews(final DSLContext conn) {
        this.conn = conn;
        this.asView = new LinkedHashMap<>();
        this.asQuery = new LinkedHashMap<>();
        initialPlacementInputPods();
        initialPlacementFixedPods();
        podsToAssign("PODS_TO_ASSIGN_NO_LIMIT");
        podsWithPortRequests();
        podNodeSelectorMatches();
        spareCapacityPerNode();
        podsThatTolerateNodeTaints();
        nodesThatHaveTolerations();
        allowedNodes();
        interPodAffinityAndAntiAffinitySimple();
    }

    /*
     * Select all pods that need to be scheduled.
     * We also indicate boolean values to check whether
     * a pod has node selector or pod affinity labels,
     * and whether pod affinity rules already yields some subset of
     * nodes that we can assign pods to.
     */
    private void initialPlacementInputPods() {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT";
        final String query = "SELECT pod_info.*, node_name AS controllable__node_name FROM pod_info " +
                             "WHERE status = 'Pending' AND node_name IS NULL AND schedulerName = 'dcm-scheduler'";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Assigned pods
     */
    private void initialPlacementFixedPods() {
        final String name = "ASSIGNED_PODS";
        final String query = "SELECT * FROM pod_info WHERE node_name IS NOT NULL";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Select top 50 pods to be scheduled.
     */
    private void podsToAssign(final String inputTable) {
        final String name = "PODS_TO_ASSIGN";
        final String query = "SELECT * FROM " + inputTable;
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Pods with port requests
     */
    private void podsWithPortRequests() {
        final String name = "PODS_WITH_PORT_REQUESTS";
        final String query =
                "SELECT pods_to_assign.controllable__node_name AS controllable__node_name, " +
                "       pod_ports_request.host_port AS host_port, " +
                "       pod_ports_request.host_ip AS host_ip, " +
                "       pod_ports_request.host_protocol AS host_protocol " +
                "FROM pods_to_assign " +
                "JOIN pod_ports_request " +
                "     ON pod_ports_request.pod_uid = pods_to_assign.uid";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Pod node selectors
     */
    private void podNodeSelectorMatches() {
        final String name = "POD_NODE_SELECTOR_MATCHES";
        final String query =
                "SELECT pods_to_assign.uid AS pod_uid, " +
                "       node_labels.node_name AS node_name " +
                "FROM pods_to_assign " +
                "JOIN pod_node_selector_labels " +
                "     ON pods_to_assign.uid = pod_node_selector_labels.pod_uid " +
                "JOIN node_labels " +
                "        ON " +
                "           (pod_node_selector_labels.label_operator = 'In' " +
                "            AND pod_node_selector_labels.label_key = node_labels.label_key " +
                "            AND pod_node_selector_labels.label_value = node_labels.label_value) " +
                "        OR (pod_node_selector_labels.label_operator = 'Exists' " +
                "            AND pod_node_selector_labels.label_key = node_labels.label_key) " +
                "        OR (pod_node_selector_labels.label_operator = 'NotIn') " +
                "        OR (pod_node_selector_labels.label_operator = 'DoesNotExist') " +
                "WHERE pods_to_assign.has_node_selector_labels = true " +
                "GROUP BY pods_to_assign.uid, node_labels.node_name, pod_node_selector_labels.term, " +
                "         pod_node_selector_labels.label_operator, pod_node_selector_labels.num_match_expressions " +
                "HAVING CASE pod_node_selector_labels.label_operator " +
                "            WHEN 'NotIn' " +
                "                 THEN NOT(ANY(pod_node_selector_labels.label_key = node_labels.label_key " +
                "                              AND pod_node_selector_labels.label_value = node_labels.label_value)) " +
                "            WHEN 'DoesNotExist' " +
                "                 THEN NOT(ANY(pod_node_selector_labels.label_key = node_labels.label_key)) " +
                "            ELSE count(distinct match_expression) = pod_node_selector_labels.num_match_expressions " +
                "       END";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Spare capacity per node
     */
    private void spareCapacityPerNode() {
        final String name = "SPARE_CAPACITY_PER_NODE";
        final String query = "SELECT name AS name, " +
                            "  cpu_allocatable - cpu_allocated AS cpu_remaining, " +
                            "  memory_allocatable - memory_allocated AS memory_remaining, " +
                            "  pods_allocatable - pods_allocated AS pods_remaining " +
                            "FROM node_info " +
                            "WHERE unschedulable = false AND " +
                            "      memory_pressure = false AND " +
                            "      out_of_disk = false AND " +
                            "      disk_pressure = false AND " +
                            "      pid_pressure = false AND " +
                            "      network_unavailable = false AND " +
                            "      ready = true AND " +
                            "      cpu_allocated < cpu_allocatable AND " +
                            "      memory_allocated <  memory_allocatable AND " +
                            "      pods_allocated < pods_allocatable; ";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Spare capacity per node
     */
    private void podsThatTolerateNodeTaints() {
        final String name = "PODS_THAT_TOLERATE_NODE_TAINTS";
        final String query = "SELECT pods_to_assign.uid AS pod_uid, " +
                             "       A.node_name AS node_name " +
                             "FROM pods_to_assign " +
                             "JOIN pod_tolerations " +
                             "     ON pods_to_assign.uid = pod_tolerations.pod_uid " +
                             "JOIN (SELECT *, COUNT(*) OVER (PARTITION BY node_name) AS num_taints " +
                             "      FROM node_taints) AS A " +
                             "     ON pod_tolerations.tolerations_key = A.taint_key " +
                             "     AND (pod_tolerations.tolerations_effect = null " +
                             "          OR pod_tolerations.tolerations_effect = A.taint_effect) " +
                             "     AND (pod_tolerations.tolerations_operator = 'Exists' " +
                             "          OR pod_tolerations.tolerations_value = A.taint_value) " +
                             "GROUP BY pod_tolerations.pod_uid, A.node_name, A.num_taints " +
                             "HAVING COUNT(*) = A.num_taints ";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Spare capacity per node
     */
    private void nodesThatHaveTolerations() {
        final String name = "NODES_THAT_HAVE_TOLERATIONS";
        final String query = "SELECT distinct node_name FROM node_taints";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Avoid overloaded nodes or nodes that report being under resource pressure
     */
    private void allowedNodes() {
        final String name = "ALLOWED_NODES";
        final String query = "SELECT name FROM spare_capacity_per_node;";
        final Supplier<CreateViewFinalStep> createView = () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createView);
    }

    /*
     * Inter-pod affinity and anti-affinity
     */
    public void interPodAffinityAndAntiAffinitySimple() {
        // The below query is simpler than it looks.
        //
        // All it does is the following:
        //   * A = join the set of pending pods with their configured <affinity/anti-affinity> match-expressions
        //   * B = join the set of <pending | fixed> pods with the set of pod labels
        //   * C = join A and C to match the set of pending pods with <pending/fixed> pods
        //         they are <affine/anti-affine> to
        //   * D = for each pending pod, produce the set of pods and nodes they are <affine / anti-affine> to
        //
        // The format string parameterizes the <pending/fixed> pods and whether we are producing the
        // result for <affinity/anti-affinity>
        final String formatString =
                        "SELECT " +
                        "  pods_to_assign_A.uid as pod_uid, " +
                        "  ARRAY_AGG(pod_labels.pod_uid) OVER (PARTITION BY pods_to_assign_A.uid) AS pod_matches, " +
                        "  ARRAY_AGG(B.node_name) OVER (PARTITION BY pods_to_assign_A.uid) AS node_matches " +
                        "FROM " +
                        "  pods_to_assign AS pods_to_assign_A " +
                        "  JOIN pod_%1$s_match_expressions ON " +
                        "        pods_to_assign_A.uid = pod_%1$s_match_expressions.pod_uid " +
                        "  JOIN pod_labels ON ( " +
                        "         (pod_%1$s_match_expressions.label_operator = 'Exists' " +
                        "         AND pod_%1$s_match_expressions.label_key = pod_labels.label_key) " +
                        "     OR (pod_%1$s_match_expressions.label_operator = 'In' " +
                        "         AND pod_%1$s_match_expressions.label_key = pod_labels.label_key " +
                        "         AND pod_labels.label_value in (unnest(pod_%1$s_match_expressions.label_value)))" +
                        "    ) " +
                        "  JOIN %2$s as B ON " +
                        "           pod_labels.pod_uid = B.uid AND pods_to_assign_A.uid != B.uid " +
                        "  WHERE pods_to_assign_A.has_pod_%1$s_requirements = true " +
                        "GROUP BY " +
                        "  pods_to_assign_A.uid, " +
                        "  pod_labels.pod_uid, " +
                        "  label_selector, " +
                        "  topology_key, " +
                        "  label_operator, " +
                        "  num_match_expressions, " +
                        "  B.node_name " +
                        "HAVING " +
                        "  COUNT(distinct match_expression) = num_match_expressions";
        for (final String type: List.of("affinity", "anti_affinity")) {
            final String pendingQuery = String.format(formatString, type, "pods_to_assign");
            final String scheduledQuery = String.format(formatString, type, "assigned_pods");
            registerQuery(pendingQuery, String.format("INTER_POD_%s_MATCHES_PENDING",
                          type.toUpperCase(Locale.ROOT)));
            registerQuery(scheduledQuery, String.format("INTER_POD_%s_MATCHES_SCHEDULED",
                          type.toUpperCase(Locale.ROOT)));
        }
    }

    void registerQuery(final String query, final String name) {
        final Supplier<CreateViewFinalStep> createViewExists =
                () -> conn.createOrReplaceView(name).as(query);
        asQuery.put(name, () -> conn.fetch(query));
        asView.put(name, createViewExists);
    }

    void initializeViews() {
        asView.forEach(
                (k, v) -> {
                    final CreateViewFinalStep createViewFinalStep = v.get();
                    System.out.println("EXECUTING  " + createViewFinalStep);
                    createViewFinalStep.execute();
                }
        );
    }

    Result<? extends Record> runQuery(final String name) {
        return asQuery.get(name).get();
    }
}