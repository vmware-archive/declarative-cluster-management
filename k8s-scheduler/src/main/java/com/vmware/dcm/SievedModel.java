/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.k8s.generated.Tables;
import org.jooq.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import static org.jooq.impl.DSL.field;

/**
 * A Sieve to reduce the Model's input.
 * TODO: expand description
 */

public class SievedModel {
    // TODO: add extra limitPriority (provided as a hint)
    // TODO: divide limitTune to three categories according to affinity
    // TODO: actually tune limitTune
    private double limitTune = 1.0;

    // TODO: add tunable sorting weights
//    private double sortWeightCpu = 0.8;
//    private double sortWeightMemory = 0.2;

    // TODO: capture successful schedules history

    private final DSLContext conn;
    private final Model model;

    SievedModel(final DSLContext conn, final Model model) {
        this.conn = conn;
        this.model = model;
    }

    private Condition getWhereClause() {
        // TODO: affinity
        return Tables.SPARE_CAPACITY_PER_NODE.PODS_REMAINING.gt(0L);
    }

// TODO: choose one of the two options
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
        // int podsCount = podsToAssignResult.size(); // TODO: Is this much faster? Is it worth it?
        int podsCount = conn.fetchCount(Tables.PODS_TO_ASSIGN_NO_LIMIT);
        return (int) Math.ceil(limitTune * podsCount);
    }

    private Function<Table<?>, Result<? extends Record>> sieve() {
        System.out.println("Sieve method...");

        return (table) -> {
            // TODO: worth the opt?
//            if (table.getName().equalsIgnoreCase("pods_to_assign_no_limit"))
//                return podsToAssignResult;
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {

                System.out.println(conn.selectFrom(table).fetch().size());
                System.out.println(conn.selectFrom(table).fetch());

                Result<?> TODO_return_immediately = conn.selectFrom(table)
                        .where(getWhereClause())
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
        return model.solve(tableName, sieve());
    }
}