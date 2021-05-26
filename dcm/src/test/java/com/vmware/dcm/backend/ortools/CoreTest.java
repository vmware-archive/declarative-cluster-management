/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import com.google.ortools.sat.Literal;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        for (final int varIndex: solver.sufficientAssumptionsForInfeasibility()) {
            System.out.println(model.getBuilder().getVariables(varIndex).getName()); // correctly prints variable "v1"
        }
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
        for (final int varIndex: solver.sufficientAssumptionsForInfeasibility()) {
            System.out.println(varIndex);
            System.out.println(model.getBuilder().getVariables(varIndex).getName()); // correctly prints variable "v1"
        }
    }
}
