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
import org.junit.jupiter.api.Test;

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

        model.addGreaterOrEqual(LinearExpr.sum(new IntVar[]{i1, i2}), 11).onlyEnforceIf(v1); // can't be satisfied
        model.addLessOrEqual(LinearExpr.sum(new IntVar[]{i1, i2}), 5).onlyEnforceIf(v2);
        model.addAssumption(v1);
        model.addAssumption(v2);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus status = solver.solve(model);

        assertEquals(CpSolverStatus.INFEASIBLE, status);
        for (final int varIndex: solver.sufficientAssumptionsForInfeasibility()) {
            System.out.println(varIndex);
            System.out.println(model.getBuilder().getVariables(varIndex).getName()); // correctly prints variable "v1"
        }
    }
}
