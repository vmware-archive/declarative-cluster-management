/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.jooq.DSLContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
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
 * The {@link Scheduler} uses an instance of this class to get the parameterized query, corresponding to the views
 * required for the respective DCM models.
 */
public class DBViews {
    private final DSLContext conn;
    private final ViewSet initialPlacement = new ViewSet("pods_to_assign", "assigned_pods");
    public final ViewSet preemption = new ViewSet("pods_to_assign_preempt", "assigned_pods_preempt");

    public DBViews(final DSLContext conn) {
        this.conn = conn;
        allPendingPods(initialPlacement);
        initialPlacementInputPods(initialPlacement);
        initialPlacementFixedPods(initialPlacement);
        preemptionInputPods(preemption);
        preemptionFixedPods(preemption);
        Stream.of(initialPlacement, preemption).forEach(viewSet -> {
            podsWithPortRequests(viewSet);
            podNodeSelectorMatches(viewSet);
            spareCapacityPerNode(viewSet);
            podsThatTolerateNodeTaints(viewSet);
            nodesThatHaveTolerations(viewSet);
            allowedNodes(viewSet);
            interPodAffinityAndAntiAffinitySimple(viewSet);
        });
    }

    /*
     * Select all pods that are pending placement.
     */
    private void allPendingPods(final ViewSet viewSet) {
        final String name = "PODS_TO_ASSIGN_NO_LIMIT";
        final String query = "SELECT pod_info.*, node_name AS controllable__node_name FROM pod_info " +
                "WHERE status = 'Pending' AND node_name IS NULL AND schedulerName = 'dcm-scheduler'";
        viewSet.addQuery(name, query);
    }

    /*
     * Select 50 pods that need to be considered for initial placement.
     */
    private void initialPlacementInputPods(final ViewSet viewSet) {
        final String name = "PODS_TO_ASSIGN";
        final String query = "SELECT * FROM PODS_TO_ASSIGN_NO_LIMIT LIMIT 50";
        viewSet.addQuery(name, query);
    }

    /*
     * Pods that are already assigned to nodes
     */
    private void initialPlacementFixedPods(final ViewSet viewSet) {
        final String name = "ASSIGNED_PODS";
        final String query = "SELECT * FROM pod_info WHERE node_name IS NOT NULL";
        viewSet.addQuery(name, query);
    }

    /*
     * Select all pods that need to be scheduled, as well as ones that can be reassigned.
     * TODO: filter by priority
     */
    private void preemptionInputPods(final ViewSet viewSet) {
        final String name = "PODS_TO_ASSIGN_PREEMPT";
        final String query = "(SELECT pod_info.*, node_name AS controllable__node_name FROM pod_info " +
                         "WHERE status = 'Pending' AND node_name IS NULL AND schedulerName = 'dcm-scheduler' " +
                         "LIMIT 50) " +
                         "UNION ALL " +
                         "(SELECT *, node_name AS controllable__node_name FROM pod_info WHERE node_name IS NOT NULL)";
        viewSet.addQuery(name, query);
    }

    /*
     * For preemption, we consider all pods are potentially reassignable. Therefore, the set of fixed pods is empty.
     * TODO: filter by priority
     */
    private void preemptionFixedPods(final ViewSet viewSet) {
        final String name = "ASSIGNED_PODS_PREEMPT";
        final String query = "SELECT * FROM pod_info LIMIT 0";
        viewSet.addQuery(name, query);
    }

    /*
     * Pods with port requests
     */
    private void podsWithPortRequests(final ViewSet viewSet) {
        final String name = "PODS_WITH_PORT_REQUESTS";
        final String query = String.format(
                "SELECT pods_to_assign.controllable__node_name AS controllable__node_name, " +
                "       pod_ports_request.host_port AS host_port, " +
                "       pod_ports_request.host_ip AS host_ip, " +
                "       pod_ports_request.host_protocol AS host_protocol " +
                "FROM %s AS pods_to_assign " +
                "JOIN pod_ports_request " +
                "     ON pod_ports_request.pod_uid = pods_to_assign.uid", viewSet.unfixedPods);
        viewSet.addQuery(name, query);
    }

    /*
     * For each pod, get the nodes that match its node selector labels.
     */
    private void podNodeSelectorMatches(final ViewSet viewSet) {
        final String name = "POD_NODE_SELECTOR_MATCHES";
        final String query = String.format(
                "SELECT pods_to_assign.uid AS pod_uid, " +
                "       node_labels.node_name AS node_name " +
                "FROM %s AS pods_to_assign " +
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
                "       END", viewSet.unfixedPods);
        viewSet.addQuery(name, query);
    }

    /*
     * Spare capacity per node
     */
    private void spareCapacityPerNode(final ViewSet viewSet) {
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
        viewSet.addQuery(name, query);
    }

    /*
     * For each pod, extract nodes that match according to taints/tolerations.
     */
    private void podsThatTolerateNodeTaints(final ViewSet viewSet) {
        final String name = "PODS_THAT_TOLERATE_NODE_TAINTS";
        final String query = String.format(
                             "SELECT pods_to_assign.uid AS pod_uid, " +
                             "       A.node_name AS node_name " +
                             "FROM %s AS pods_to_assign " +
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
                             "HAVING COUNT(*) = A.num_taints ", viewSet.unfixedPods);
        viewSet.addQuery(name, query);
    }

    /*
     * The set of nodes that have any tolerations configured
     */
    private void nodesThatHaveTolerations(final ViewSet viewSet) {
        final String name = "NODES_THAT_HAVE_TOLERATIONS";
        final String query = "SELECT distinct node_name FROM node_taints";
        viewSet.addQuery(name, query);
    }

    /*
     * Avoid overloaded nodes or nodes that report being under resource pressure
     */
    private void allowedNodes(final ViewSet viewSet) {
        final String name = "ALLOWED_NODES";
        final String query = "SELECT name FROM spare_capacity_per_node;";
        viewSet.addQuery(name, query);
    }

    /*
     * Inter-pod affinity and anti-affinity
     */
    public void interPodAffinityAndAntiAffinitySimple(final ViewSet viewSet) {
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
                        "SELECT DISTINCT" +
                        "  pods_to_assign_A.uid as pod_uid, " +
                        "  ARRAY_AGG(pod_labels.pod_uid) OVER (PARTITION BY pods_to_assign_A.uid) AS pod_matches, " +
                        "  ARRAY_AGG(B.node_name) OVER (PARTITION BY pods_to_assign_A.uid) AS node_matches " +
                        "FROM " +
                        "  %2$s AS pods_to_assign_A " +
                        "  JOIN pod_%1$s_match_expressions ON " +
                        "        pods_to_assign_A.uid = pod_%1$s_match_expressions.pod_uid " +
                        "  JOIN pod_labels ON ( " +
                        "         (pod_%1$s_match_expressions.label_operator = 'Exists' " +
                        "         AND pod_%1$s_match_expressions.label_key = pod_labels.label_key) " +
                        "     OR (pod_%1$s_match_expressions.label_operator = 'In' " +
                        "         AND pod_%1$s_match_expressions.label_key = pod_labels.label_key " +
                        "         AND pod_labels.label_value in (unnest(pod_%1$s_match_expressions.label_value)))" +
                        "    ) " +
                        "  JOIN %3$s as B ON " +
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
            final String pendingQuery = String.format(formatString, type, viewSet.unfixedPods, viewSet.unfixedPods);
            final String scheduledQuery = String.format(formatString, type, viewSet.unfixedPods, viewSet.fixedPods);
            viewSet.addQuery(String.format("INTER_POD_%s_MATCHES_PENDING", type.toUpperCase(Locale.ROOT)),
                             pendingQuery);
            viewSet.addQuery(String.format("INTER_POD_%s_MATCHES_SCHEDULED", type.toUpperCase(Locale.ROOT)),
                             scheduledQuery);
        }
    }

    @SuppressWarnings("ReturnValueIgnored")
    void initializeViews() {
        final BiFunction<String, String, Integer> createView =
                (name, query) -> conn.createView(name).as(query).execute();

        // By default, all views refer to their initial placement versions
        initialPlacement.asQuery.forEach(createView::apply);

        // Create views for the preemption version of fixed/unfixed pods
        preemption.asQuery.forEach(
                (name, query) -> {
                    if (name.equals("PODS_TO_ASSIGN_PREEMPT") || name.equals("ASSIGNED_PODS_PREEMPT")) {
                        createView.apply(name, query);
                    }
                }
        );
    }

    static class ViewSet {
        private final String unfixedPods;
        private final String fixedPods;
        private final Map<String, String> asQuery = new LinkedHashMap<>();

        ViewSet(final String unfixedPods, final String fixedPods) {
            this.unfixedPods = unfixedPods;
            this.fixedPods = fixedPods;
        }

        void addQuery(final String name, final String query) {
            asQuery.put(name, query);
        }

        public String getQuery(final String name) {
            return asQuery.get(name.toUpperCase());
        }
    }
}