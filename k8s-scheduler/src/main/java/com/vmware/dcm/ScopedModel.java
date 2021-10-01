/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.collect.Sets;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SortField;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.selectFrom;
import static org.jooq.impl.DSL.table;

/**
 * A Scope to reduce the Model's solver input.
 *
 * TODO: maintain different limitTune variables according to affinity
 *       add extra limitPriority (provided as a hint)
 *       capture successful schedules history
 */
public class ScopedModel {
    private static final Logger LOG = LoggerFactory.getLogger(ScopedModel.class);
    private static final double LIMIT_TUNE_DEFAULT = 1.0;

    private final DSLContext conn;
    private final Model model;

    ScopedModel(final DSLContext conn, final Model model) {
        this.conn = conn;
        this.model = model;
    }

    /**
     * Returns a where predicate to filter a given set of nodes.
     *
     * @param nodeSet Set of nodes to survive the filtering
     * @return Where clause predicate
     */
    private Condition getWherePredicate(final Set<String> nodeSet) {
        return field(name("SPARE_CAPACITY_PER_NODE", "NAME")).in(nodeSet);
    }

    /**
     * Computes a sorting expression for an ORDER BY clause, based on a sorting strategy.
     * This sorting strategy favors nodes with more spare capacity of a defined resource.
     *
     * @return Sorting expression
     */
    private SortField<?> getSortingExpression() {
        return field("capacity").desc();
    }

    /**
     * Limits the number of candidates nodes fetched for a set of pods.
     * The number of nodes is proportional to the number of pods considered during the invocation.
     * This nodes to pods ratio will be tuned dynamically.
     *
     * @param podCnt Number of pods consider during invocation
     * @return Number of nodes to consider
     */
    private int getLimit(final int podCnt) {
        return podCnt == 0 ? 1 : (int) Math.ceil(LIMIT_TUNE_DEFAULT * podCnt);
    }

    /**
     * Returns the set of candidate nodes for pods with selector labels.
     *
     * @return Set of candidate nodes
     */
    private Set<String> getMatchedNodes() {
        final Result<?> podNodeSelectorMatches = conn.selectFrom(table("POD_NODE_SELECTOR_MATCHES")).fetch();
        final Set<Object[]> setOfNodeLists = podNodeSelectorMatches
                .intoSet(field(name("POD_NODE_SELECTOR_MATCHES", "NODE_MATCHES"), Object[].class));
        return flattenObjectArray(setOfNodeLists);
    }

    /**
     * Returns the set of candidate nodes for pods with inter-pod-affinity constraints
     *
     * @return Set of candidate nodes
     */
    private Set<String> getPodAffinityNodes() {
        final Result<?> affinityNodeMatchesPending = conn.select(field("NODE_MATCHES")).from(
                table("INTER_POD_AFFINITY_MATCHES_PENDING")).fetch();
        final Result<?> affinityNodeMatchesScheduled = conn.select(field("NODE_MATCHES")).from(
                table("INTER_POD_AFFINITY_MATCHES_SCHEDULED")).fetch();

        return Sets.union(
                affinityNodeMatchesPending
                        .stream()
                        .map(x -> Optional.ofNullable(x.get(0, String[].class)))
                        .flatMap(x -> x.stream().flatMap(Arrays::stream))
                        .collect(Collectors.toSet()),
                affinityNodeMatchesScheduled
                        .stream()
                        .map(x -> Optional.ofNullable(x.get(0, String[].class)))
                        .flatMap(x -> x.stream().flatMap(Arrays::stream))
                        .collect(Collectors.toSet())
        );
    }

    /**
     * Returns the set of tainted nodes that are tolerated by pods to be assigned.
     *
     * @return Set of candidate nodes
     */
    private Set<String> getToleratedNodes() {
        final Result<?> toleratingPods = conn.selectFrom(table("PODS_THAT_TOLERATE_NODE_TAINTS")).fetch();
        return toleratingPods.intoSet(field(name("PODS_THAT_TOLERATE_NODE_TAINTS", "NODE_NAME"), String.class));
    }

    private Set<String> flattenObjectArray(final Set<Object[]> arrays) {
        return arrays.stream().flatMap(Arrays::stream).map(Object::toString).collect(Collectors.toSet());
    }

    /**
     * Returns a set including the least loaded nodes.
     * This set is computed as the union the least loaded nodes across all resource types.
     * Load is defined by the sorting strategy in {@link #getSortingExpression()}.
     * Each resource's set cardinality is defined by {@link #getLimit(int)}.
     *
     * @return Set of least loaded nodes
     */
    private Set<String> getSpareNodes() {
        final int podsCnt = conn.fetchCount(selectFrom(table("PODS_TO_ASSIGN")));

        // find resource types available in nodes
        final Result<? extends Record1<?>> resources = conn.selectDistinct(field("resource"))
                .from(table("SPARE_CAPACITY_PER_NODE")).fetch();

        return resources.stream().map((resource) ->
                conn.selectFrom("SPARE_CAPACITY_PER_NODE")
                        .where(field(name("SPARE_CAPACITY_PER_NODE", "RESOURCE")).eq(resource.value1().toString()))
                        .orderBy(getSortingExpression())
                        .limit(getLimit(podsCnt))
                        .fetch().intoSet(name("SPARE_CAPACITY_PER_NODE", "NAME"), String.class)
        ).flatMap(Set::stream).collect(Collectors.toSet());
    }

    /**
     * Gets the final set of scoped nodes as a union.
     *
     * @return Set of node names
     */
    public Set<String> getScopedNodes() {
        return Stream.of(
                getMatchedNodes(),
                getPodAffinityNodes(),
                getToleratedNodes(),
                getSpareNodes()
        ).flatMap(Set::stream).collect(Collectors.toSet());
    }

    /**
     * Applies the scope logic to filter the nodes table.
     *
     * @return A function object to apply the scope filtering
     */
    public Function<Table<?>, Result<? extends Record>> scope() {
        return (table) -> {
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {
                final long start = System.nanoTime();

                final Result<?> scopedFetcher = conn.selectFrom(table)
                        .where(getWherePredicate(getScopedNodes()))
                        .fetch();

                LOG.info("Scope filtering latency: {}ns", (System.nanoTime() - start));

                // TODO: Used only for log info. Remove if expensive to compute.
                final int sizeUnfiltered = conn.selectFrom(table).fetch().size();
                final int sizeScoped = scopedFetcher.size();
                LOG.info("Solver input size without scope: {}\n" +
                                "solver input size with scope: {}\n" +
                                "scope fraction: {}",
                        sizeUnfiltered, sizeScoped, (double) sizeScoped / sizeUnfiltered);
                return scopedFetcher;
            } else {
                return conn.fetch(table);
            }
        };
    }

    /**
     * Calls the {@link #model}'s {@link Model#solve(String)} method with the scoped input.
     *
     * @param tableName a table name
     * @return A Result object representing rows of the corresponding tables, with modifications made by the solver
     */
    public Result<? extends Record> solve(final String tableName) {
        return model.solve(tableName, scope());
    }
}
