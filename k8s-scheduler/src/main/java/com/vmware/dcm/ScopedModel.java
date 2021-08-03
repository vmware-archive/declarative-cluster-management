/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.collect.Sets;
import com.vmware.dcm.k8s.generated.Tables;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SortField;
import org.jooq.Table;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.selectFrom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Function;

/**
 * A Scope to reduce the Model's solver input.
 *
 * TODO: dynamically tune the now default parameters {LIMIT_TUNE, CPU_WEIGHT, MEM_WEIGHT}_DEFAULT
 *       maintain (three) different limitTune variables according to affinity
 *       add extra limitPriority (provided as a hint)
 *       capture successful schedules history
 */
public class ScopedModel {
    private static final Logger LOG = LoggerFactory.getLogger(ScopedModel.class);
    private static final double LIMIT_TUNE_DEFAULT = 1.0;
    private static final double CPU_WEIGHT_DEFAULT = 0.8;
    private static final double MEM_WEIGHT_DEFAULT = 0.2;

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
        return Tables.SPARE_CAPACITY_PER_NODE.NAME.in(nodeSet);
    }

    /**
     * Computes a sorting expression for an ORDER BY clause, based on a sorting strategy.
     * This sorting strategy sorts according to a weighted sum of nodes' spare cpu and memory.
     *
     * @return Sorting expression
     */
    private SortField<?> getSortingExpression() {
        return field(CPU_WEIGHT_DEFAULT + " * cpu_remaining + "
                + MEM_WEIGHT_DEFAULT + " * memory_remaining").desc();
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
        return (int) Math.ceil(LIMIT_TUNE_DEFAULT * podCnt);
    }

    /**
     * Returns the set of candidate nodes for pods with selector labels.
     * TODO: Cover more constraints
     *
     * @return Set of candidate nodes
     */
    private Set<String> getMatchedNodes() {
        final Result<?> podNodeSelectorMatches = conn.selectFrom(Tables.POD_NODE_SELECTOR_MATCHES).fetch();

        return podNodeSelectorMatches.intoSet(Tables.POD_NODE_SELECTOR_MATCHES.NODE_NAME);
    }

    /**
     * Returns a set with the least loaded nodes.
     * Load is defined by the sorting strategy in {@link #getSortingExpression()}.
     * Set cardinality is defined by {@link #getLimit(int)}.
     *
     * @return Set of least loaded nodes
     */
    private Set<String> getSpareNodes() {
        final int podsCnt = conn.fetchCount(selectFrom(Tables.PODS_TO_ASSIGN));
        final Result<?> spareNodes = conn.selectFrom(Tables.SPARE_CAPACITY_PER_NODE)
                .orderBy(getSortingExpression())
                .limit(getLimit(podsCnt))
                .fetch();
        return spareNodes.intoSet(Tables.SPARE_CAPACITY_PER_NODE.NAME);
    }

    /**
     * Gets the final set of scoped nodes as a union.
     *
     * @return Set of node names
     */
    public Set<String> getScopedNodes() {
        return Sets.union(getMatchedNodes(), getSpareNodes());
    }

    /**
     * Applies the scope logic to filter the nodes table.
     *
     * @return A function object to apply the scope filtering
     */
    public Function<Table<?>, Result<? extends Record>> scope() {
        return (table) -> {
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {
                // TODO: Used only for log info. Remove if expensive to compute.
                final int sizeUnfiltered = conn.selectFrom(table).fetch().size();

                final Result<?> scopedFetcher = conn.selectFrom(table)
                        .where(getWherePredicate(getScopedNodes()))
                        .fetch();

                final int sizeFiltered = scopedFetcher.size();
                LOG.info("solver input size without scope: {}\n" +
                                "solver input size with scope: {}\n" +
                                "scope fraction: {}",
                        sizeUnfiltered, sizeFiltered, (double) sizeFiltered / sizeUnfiltered);
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