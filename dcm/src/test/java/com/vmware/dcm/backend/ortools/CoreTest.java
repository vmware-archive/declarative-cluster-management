/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.IntervalVar;
import com.google.ortools.sat.LinearExpr;
import com.vmware.dcm.Model;
import com.vmware.dcm.SolverException;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CoreTest {
    static {
        // Causes or-tools JNI library to be loaded
        new OrToolsSolver.Builder().build();
    }

    @Test
    public void assumptionsTest() {
        final CpModel model = new CpModel();
        final IntVar i1 = model.newIntVar(0, 5, "i1");
        final IntVar i2 = model.newIntVar(0, 5, "i2");
        final IntVar v1 = model.newBoolVar("v1");
        final IntVar v2 = model.newBoolVar("v2");
        final IntVar v3 = model.newBoolVar("v3");

        model.addGreaterOrEqual(LinearExpr.sum(new IntVar[]{i1, i2}), 11).onlyEnforceIf(v1); // can't be satisfied
        model.addLessOrEqual(LinearExpr.sum(new IntVar[]{i1, i2}), 5).onlyEnforceIf(v2);
        model.addEquality(v3, 1);
        model.addAssumption(v1);
        model.addAssumption(v2);
        model.addAssumption(v3);
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(1);
        final CpSolverStatus status = solver.solve(model);

        assertEquals(CpSolverStatus.INFEASIBLE, status);
        assertTrue(solver.sufficientAssumptionsForInfeasibility()
                .stream()
                .map(e -> model.getBuilder().getVariables(e).getName())
                .collect(Collectors.toList())
                .contains("v1"));
    }

    @Test
    public void cumulative() {
        final CpModel model = new CpModel();
        final IntVar origin = model.newIntVar(0, 5, "i1");
        final IntVar i1 = model.newIntVar(0, 5, "i1o");
        final IntVar assumptionVar1 = model.newBoolVar("Assumption 1");
        model.addEquality(origin, i1).onlyEnforceIf(assumptionVar1);
        model.addDifferent(origin, i1).onlyEnforceIf(assumptionVar1.not());
        final IntervalVar[] tasksIntervals = new IntervalVar[1];
        tasksIntervals[0] = model.newOptionalIntervalVar(i1, model.newConstant(1),  model.newConstant(1),
                assumptionVar1, "");

        // Can't be satisfied
        model.addCumulative(tasksIntervals, new IntVar[]{model.newConstant(11)}, model.newConstant(10));
        model.addAssumption(assumptionVar1);

        final IntVar i2 = model.newIntVar(0, 5, "i2");
        final IntVar i3 = model.newIntVar(0, 5, "i3");
        final IntVar assumptionVar2 = model.newBoolVar("Assumption 2");
        final IntVar assumptionVar3 = model.newBoolVar("Assumption 3");

        // Can't be satisfied
        model.addGreaterOrEqual(LinearExpr.sum(new IntVar[]{i2, i3}), 11).onlyEnforceIf(assumptionVar2);
        model.addLessOrEqual(LinearExpr.sum(new IntVar[]{i2, i3}), 5).onlyEnforceIf(assumptionVar3);
        model.addAssumption(assumptionVar2);
        model.addAssumption(assumptionVar3);

        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(1);
        final CpSolverStatus status = solver.solve(model);

        assertEquals(CpSolverStatus.INFEASIBLE, status);
        assertTrue(solver.sufficientAssumptionsForInfeasibility()
                .stream()
                .map(e -> model.getBuilder().getVariables(e).getName())
                .collect(Collectors.toList())
                .contains("Assumption 1"));
    }

    @Test
    public void assumptionsTestWithOps() {
        final CpModel model = new CpModel();
        final StringEncoding encoding = new StringEncoding();
        final Ops o = new Ops(model, encoding);
        final IntVar i1 = model.newIntVar(0, 5, "i1");
        final IntVar i2 = model.newIntVar(0, 5, "i2");
        final IntVar i3 = model.newIntVar(0, 5, "i2");

        o.assume(o.eq(i1, 3), "i1 constraint_all_different");
        o.assume(model.newConstant(1), "i2 constraint_all_different");
        o.assume(model.newConstant(1), "i3 constraint_all_different");

        o.assume(o.and(o.leq(i1, 2), o.geq(i1, 1)), "i1 constraint_domain");
        o.assume(o.and(o.leq(i2, 2), o.geq(i2, 1)), "i2 constraint_domain");
        o.assume(o.and(o.leq(i3, 2), o.geq(i3, 1)), "i3 constraint_domain");
        final CpSolver solver = new CpSolver();
        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setNumSearchWorkers(1);
        final CpSolverStatus status = solver.solve(model);

        assertEquals(CpSolverStatus.INFEASIBLE, status);
        assertTrue(solver.sufficientAssumptionsForInfeasibility()
                .stream()
                .map(e -> model.getBuilder().getVariables(e).getName())
                .collect(Collectors.toList())
                .containsAll(List.of("i1 constraint_all_different", "i1 constraint_domain")));
    }

    @Test
    public void allDifferentInfeasibility() {
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        conn.execute("create table t1(id integer, controllable__var integer)");
        conn.execute("insert into t1 values (1, null)");
        conn.execute("insert into t1 values (2, null)");
        conn.execute("insert into t1 values (3, null)");

        // Unsatisfiable
        final String allDifferent = "create view constraint_all_different as " +
                "select * from t1 check all_different(controllable__var) = true";

        // Unsatisfiable
        final String domain1 = "create view constraint_domain_1 as " +
                "select * from t1 check controllable__var >= 1 and controllable__var <= 2";

        // Satisfiable
        final String domain2 = "create view constraint_domain_2 as " +
                "select * from t1 check id != 1 or controllable__var = 1";

        final Model model = Model.build(conn, List.of(allDifferent, domain1, domain2));
        model.updateData();
        try {
            model.solve("T1");
            fail();
        } catch (final SolverException exception) {
            assertTrue(exception.core().containsAll(List.of("constraint_all_different", "constraint_domain_1")));
            assertFalse(exception.core().contains("constraint_domain_2"));
        }
    }

    @Test
    public void sumInfeasibility() {
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        conn.execute("create table t1(id integer, controllable__var integer)");
        conn.execute("insert into t1 values (1, null)");
        conn.execute("insert into t1 values (2, null)");
        conn.execute("insert into t1 values (3, null)");

        // Unsatisfiable
        final String sum = "create view constraint_sum as " +
                "select * from t1 check sum(controllable__var) = 7";

        // Unsatisfiable
        final String domain1 = "create view constraint_domain_1 as " +
                "select * from t1 check controllable__var >= 1 and controllable__var <= 2";

        // Satisfiable
        final String domain2 = "create view constraint_domain_2 as " +
                "select * from t1 check id != 1 or controllable__var = 1";

        final Model model = Model.build(conn, List.of(sum, domain1, domain2));
        model.updateData();
        try {
            model.solve("T1");
            fail();
        } catch (final SolverException exception) {
            assertTrue(exception.core().containsAll(List.of("constraint_sum", "constraint_domain_1")));
            assertFalse(exception.core().contains("constraint_domain_2"));
        }
    }
}
