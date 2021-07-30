/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.k8s.generated.Tables;
import org.jooq.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.jooq.impl.DSL.*;

/**
 * A Scope to reduce the Model's input.
 * TODO: expand description
 */

// TODO: remove prints
// TODO: why no limit?

public class ScopedModel {
    // limitTune is used to adjust the number of scoped nodes
    private double limitTune = 1.0;
    // TODO: add extra limitPriority (provided as a hint)
    //       divide limitTune to three categories according to affinity
    //       actually tune limitTune

    // TODO: add tunable sorting weights
//    private double sortWeightCpu = 0.8;
//    private double sortWeightMemory = 0.2;

    // TODO: capture successful schedules history

    private final DSLContext conn;
    private final Model model;

    ScopedModel(final DSLContext conn, final Model model) {
        this.conn = conn;
        this.model = model;
    }

//    private Collection<Condition> getWhereClause() {
//        Collection<Condition> conditions = new ArrayList<>();
//        Result<?> naPods = conn.selectFrom(Tables.PODS_TO_ASSIGN_NO_LIMIT)
//                .where(Tables.PODS_TO_ASSIGN_NO_LIMIT.HAS_NODE_SELECTOR_LABELS.eq(true)).fetch();

//        Result<?> x = conn.selectFrom(Tables.POD_NODE_SELECTOR_LABELS).fetch();
        //                .fetchGroups(Tables.PODS_TO_ASSIGN_NO_LIMIT.HAS_NODE_SELECTOR_LABELS);
//                .where(Tables.PODS_TO_ASSIGN_NO_LIMIT.HAS_NODE_SELECTOR_LABELS.eq(true)).fetch();
//        y.intoGroups(Tables.PODS_TO_ASSIGN_NO_LIMIT.HAS_NODE_SELECTOR_LABELS);


    private Map<List<String>, List<String>> getNodeSets() {
        System.out.println("inside get node sets...");

        Map<String, ? extends Result<?>> nodesPerPod = conn.selectFrom(Tables.POD_NODE_SELECTOR_MATCHES).fetch()
                .intoGroups(Tables.POD_NODE_SELECTOR_MATCHES.POD_UID);

        Map<List<String>, List<String>> nodeSets = new HashMap<>();
        for (String pod : nodesPerPod.keySet()) {
            List<String> nodes = nodesPerPod.get(pod).getValues(Tables.POD_NODE_SELECTOR_MATCHES.NODE_NAME);

            if (!nodeSets.containsKey(nodes))
                nodeSets.put(nodes, new ArrayList<>());
            nodeSets.get(nodes).add(pod);
        }

        return nodeSets;
    }

// choose the commented option to tune node selection between spare cpu and memory capacity
//    private SortField<?> getSorting() {
//        return field(sortWeightCpu + " * cpu_remaining + "
//                + sortWeightMemory + " * memory_remaining").desc();
//    }
    private Collection<SortField<?>> getSorting() {
        Collection<SortField<?>> sorts = new ArrayList<>();
        sorts.add(Tables.SPARE_CAPACITY_PER_NODE.CPU_REMAINING.desc());
        sorts.add(Tables.SPARE_CAPACITY_PER_NODE.MEMORY_REMAINING.desc());

        return sorts;
    }

    private int getLimit() {
        int podsCount = conn.fetchCount(Tables.PODS_TO_ASSIGN_NO_LIMIT);
        return (int) Math.ceil(limitTune * podsCount);
    }

    private int getLimit(int cnt) {
        return (int) Math.ceil(limitTune * cnt);
    }

    private Function<Table<?>, Result<? extends Record>> scope() {
        System.out.println("scope method...");

        // get nodesets -- just strings
        Map<List<String>, List<String>> nodeSets = getNodeSets();

        Iterator<List<String>> iter = nodeSets.keySet().iterator();

        List<String> tmp_key1 = iter.next();
        List<String> tmp_key2 = iter.next();
        List<String> tmp_val1 = nodeSets.get(tmp_key1);
        List<String> tmp_val2 = nodeSets.get(tmp_key2);

        return (table) -> {
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {

                SelectOrderByStep<?> a = conn.select().from(
                        select().from(table)
                        .where(Tables.SPARE_CAPACITY_PER_NODE.NAME.in(tmp_key1))
                        .orderBy(getSorting())
                        .limit(getLimit(tmp_val1.size()))
                    .union(
                        select().from(table)
                        .where(Tables.SPARE_CAPACITY_PER_NODE.NAME.in(tmp_key2))
                        .orderBy(getSorting())
                        .limit(getLimit(tmp_val2.size()))
                        ));


                System.out.println("here");
                System.out.println(a.fetch());
                System.out.println();

//                Stream<Object> x = nodeSets.keySet().stream().map( (nodes) -> {
//                    return conn.selectFrom(table)
//                            .where(Tables.SPARE_CAPACITY_PER_NODE.NAME
//                                    .in(nodes));
//                });
//
//
//
//                int xx = x.reduce((select1, select2) -> {
//                    return select1.union(select2);
//                });

                // filter them (where)

                // sort them (order by)

                // limit them (limit)

                // union (union)


                System.out.println(conn.selectFrom(table).fetch().size());
                System.out.println(conn.selectFrom(table).fetch());

                Result<?> TODO_return_immediately = conn.selectFrom(table)
//                        .where(getWhereClause())
                        .orderBy(getSorting())
                        .limit(getLimit())
                        .fetch();

                System.out.println(TODO_return_immediately.size());
                System.out.println(TODO_return_immediately);

                return TODO_return_immediately;
            }
            else
                return conn.fetch(table);
        };
    }

    public Result<? extends Record> solve(final String tableName) {
        return model.solve(tableName, scope());
    }
}