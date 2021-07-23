/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarProductOptimizationTest {

    @ParameterizedTest
    @MethodSource("clauses")
    public void leftVarRightConst(final String checkClause, final int total) {
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        conn.execute("CREATE TABLE t1 (c1 integer, controllable__c2 integer)");
        conn.execute("insert into t1 values(1, 1)");
        conn.execute("insert into t1 values(1, 1)");
        conn.execute("insert into t1 values(1, 1)");

        final List<String> constraints = List.of("CREATE CONSTRAINT c1 AS " +
                                                 "SELECT * FROM t1 " +
                                                 "CHECK " + checkClause);
        final OrToolsSolver solver = new OrToolsSolver.Builder().setTryScalarProductEncoding(true).build();
        final Model model = Model.build(conn, solver, constraints);
        final Result<? extends Record> result = model.solve("T1");
        final int actualTotal = result.stream().map(r ->   r.get("C1", Integer.class)
                                                   * r.get("CONTROLLABLE__C2", Integer.class))
                                              .reduce(Integer::sum).get();
        assertEquals(total, actualTotal);
        assertTrue(model.compilationOutput().stream().anyMatch(e -> e.contains("o.scalProd")));
    }

    private static Stream<Arguments> clauses() {
        return Stream.of(Arguments.of("sum(controllable__c2 * c1) = 10", 10),
                         Arguments.of("sum(c1 * controllable__c2) = 10", 10),
                         Arguments.of("sum(c1 * controllable__c2 * 5) = 15", 3),
                         Arguments.of("sum(5 * c1 * controllable__c2) = 15", 3));
    }
}
