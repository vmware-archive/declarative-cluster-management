/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A Scope to reduce the Model's solver input.
 */

public class ScopedModel {
    private static final Logger LOG = LoggerFactory.getLogger(ScopedModel.class);
    private final DSLContext conn;
    private final Model model;

    // dynamically adjuct number of scoped nodes with limitTune
    private double limitTune = 1.0;
    // TODO: add extra limitPriority (provided as a hint)
    //       divide limitTune to (three) categories according to affinity
    //       actually tune limitTune

    // TODO: add testcase
    private double sortWeightCpu = 0.8;
    private double sortWeightMemory = 0.2;

    // TODO: capture successful schedules history


    ScopedModel(final DSLContext conn, final Model model) {
        this.conn = conn;
        this.model = model;
    }

    private Collection<Condition> getWhereClause(final Set<String> nodeSet) {
        final Collection<Condition> conditions = new ArrayList<>();
        conditions.add(Tables.SPARE_CAPACITY_PER_NODE.NAME.in(nodeSet));

        return conditions;
    }

    private SortField<?> getSorting() {
        return field(sortWeightCpu + " * cpu_remaining + "
                + sortWeightMemory + " * memory_remaining").desc();
    }

    private int getLimit(final int cnt) {
        return (int) Math.ceil(limitTune * cnt);
    }

    private Set<String> getMatchedNodes () {
        final Result<?> podNodeSelectorMatches = conn.selectFrom(Tables.POD_NODE_SELECTOR_MATCHES).fetch();

        return podNodeSelectorMatches.intoSet(Tables.POD_NODE_SELECTOR_MATCHES.NODE_NAME);
    }

    private Set<String> getSpareNodes() {
        final int podsCnt = conn.fetchCount(selectFrom(Tables.PODS_TO_ASSIGN));

        Result<?> spareNodes = conn.selectFrom(Tables.SPARE_CAPACITY_PER_NODE)
                .orderBy(getSorting())
                .limit(getLimit(podsCnt))
                .fetch();

        return spareNodes.intoSet(Tables.SPARE_CAPACITY_PER_NODE.NAME);
    }

    public Set<String> getScopedNodes() {
        Set<String> union = new HashSet<>();
        Stream.of(
                getMatchedNodes(),
                getSpareNodes()
        ).forEach(union::addAll);

        return union;
    }

    public Function<Table<?>, Result<? extends Record>> scope() {
        return (table) -> {
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {

                // TODO: Used only for log info. Remove if it is expensive to compute.
                final int sizeUnfiltered = conn.selectFrom(table).fetch().size();

                final Result<?> scopedFetcher = conn.selectFrom(table)
                        .where(getWhereClause(getScopedNodes()))
                        .fetch();

                final int sizeFiltered = scopedFetcher.size();
                LOG.info("solver input size without scope: {}\n" +
                        "solver input size with scope: {}\n" +
                        "scope fraction: {}",
                        sizeUnfiltered, sizeFiltered, (double) sizeFiltered / sizeUnfiltered);

                return scopedFetcher;
            }

            else {
                return conn.fetch(table);
            }
        };
    }

    public void setSortWeights(final double weightCpu, final double weightMemory) {
        sortWeightCpu = weightCpu;
        sortWeightMemory = weightMemory;
    }

    public Result<? extends Record> solve(final String tableName) {
        return model.solve(tableName, scope());
    }
}