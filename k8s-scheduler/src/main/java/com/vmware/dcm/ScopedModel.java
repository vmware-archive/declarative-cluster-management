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
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectFrom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A Scope to reduce the Model's input.
 * TODO: expand description
 */

// TODO: remove prints

// TODO: (ask): pods_to_assign_no_limit vs pods_to_assign
//       testScopedSchedulerSimple seems to be scheduling batches of 50.

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

    private Map<Set<String>, Set<String>> getNodeSets() {
        Result<?> podNodeSelectorMatches = conn.selectFrom(Tables.POD_NODE_SELECTOR_MATCHES).fetch();
        Set<String> constraintNodes = podNodeSelectorMatches.intoSet(Tables.POD_NODE_SELECTOR_MATCHES.NODE_NAME);
        Set<String> constraintPods = podNodeSelectorMatches.intoSet(Tables.POD_NODE_SELECTOR_MATCHES.POD_UID);

        System.out.println("node sets");
        System.out.println(constraintNodes.size());
        System.out.println(constraintNodes);
        System.out.println(constraintPods.size());
        System.out.println(constraintPods);

        return Map.of(constraintNodes, constraintPods);
    }

    private int getRestPodsCnt() {
        int restPodsCnt = conn.fetchCount(selectFrom(Tables.PODS_TO_ASSIGN_NO_LIMIT)
                .where(Tables.PODS_TO_ASSIGN_NO_LIMIT.HAS_NODE_SELECTOR_LABELS.eq(false)));

        System.out.println("pods not labeled");
        System.out.println(restPodsCnt);
        System.out.println(conn.selectFrom(Tables.PODS_TO_ASSIGN_NO_LIMIT)
                .where(Tables.PODS_TO_ASSIGN_NO_LIMIT.HAS_NODE_SELECTOR_LABELS.eq(false)).fetch());

        return restPodsCnt;
    }

    private Collection<Condition> getWhereClause(Set<String> nodeSet) {
        Collection<Condition> conditions = new ArrayList<>();
        conditions.add(Tables.SPARE_CAPACITY_PER_NODE.NAME.in(nodeSet));

        return conditions;
    }

    // choose the commented option to tune node selection between spare cpu and memory capacity
    private SortField<?> getSorting() {
        return field(sortWeightCpu + " * cpu_remaining + "
                + sortWeightMemory + " * memory_remaining").desc();
    }
//    private Collection<SortField<?>> getSorting() {
//        Collection<SortField<?>> sorts = new ArrayList<>();
//        sorts.add(Tables.SPARE_CAPACITY_PER_NODE.CPU_REMAINING.desc());
//        sorts.add(Tables.SPARE_CAPACITY_PER_NODE.MEMORY_REMAINING.desc());
//
//        return sorts;
//    }

//    private int getLimit() {
//        int podsCount = conn.fetchCount(Tables.PODS_TO_ASSIGN_NO_LIMIT);
//        return (int) Math.ceil(limitTune * podsCount);
//    }

    private int getLimit(int cnt) {
        return (int) Math.ceil(limitTune * cnt);
    }

    private Function<Table<?>, Result<? extends Record>> scope() {
        int restPodsCnt = getRestPodsCnt();
        Map<Set<String>, Set<String>> nodeSets = getNodeSets();
        Set<String> nodeSet = nodeSets.keySet().iterator().next();

        // TODO: union nodesets

        return (table) -> {
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {

                // TODO: Is this expensive to compute? remove?
                int sizeUnfiltered = conn.selectFrom(table).fetch().size();

                Result<?> scopedFetcher = conn.select().from(
                        select().from(table)
                        .where(getWhereClause(nodeSet))
// TODO: enable limit in separate node sets
//                        .orderBy(getSorting())
//                        .limit(getLimit(nodeSets.get(nodeSet).size()))
                    .union(
                        select().from(table)
                        .orderBy(getSorting())
                        .limit(getLimit(restPodsCnt))
                    )).fetch();

                int sizeFiltered = scopedFetcher.size();
                LOG.info("solver input size without scope: {}\n" +
                        "solver input size with scope: {}\n" +
                        "scope fraction: {}",
                        sizeUnfiltered, sizeFiltered, (double) sizeFiltered / sizeUnfiltered);

                return scopedFetcher;

//                SelectOrderByStep<?> a = conn.select().from(
//                        select().from(table)
//                        .where(Tables.SPARE_CAPACITY_PER_NODE.NAME.in(tmp_key1))
//                        .orderBy(getSorting())
//                        .limit(getLimit(tmp_val1.size()))
//                    .union(
//                        select().from(table)
//                        .where(Tables.SPARE_CAPACITY_PER_NODE.NAME.in(tmp_key2))
//                        .orderBy(getSorting())
//                        .limit(getLimit(tmp_val2.size()))
//                        ));
//
//                Stream<Object> x = nodeSets.keySet().stream().map( (nodes) -> {
//                    return conn.selectFrom(table)
//                            .where(Tables.SPARE_CAPACITY_PER_NODE.NAME
//                                    .in(nodes));
//                });
//
//                int xx = x.reduce((select1, select2) -> {
//                    return select1.union(select2);
//                });

                // filter them (where)

                // sort them (order by)

                // limit them (limit)

                // union (union)
            }
            else
                return conn.fetch(table);
        };
    }

    public void setSortWeights(double weightCpu, double weightMemory) {
        sortWeightCpu = weightCpu;
        sortWeightMemory = weightMemory;
    }

    public Result<? extends Record> solve(final String tableName) {
        return model.solve(tableName, scope());
    }
}