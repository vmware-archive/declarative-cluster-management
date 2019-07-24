/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.DecisionStrategyProto;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import com.google.ortools.sat.SatParameters;
import com.google.ortools.util.Domain;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OrToolsTest {
    static {
        System.loadLibrary("jniortools");
    }

    @Test
    public void test() {
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final int numVals = 1000;
        final IntVar[] vars = new IntVar[numVals];
        for (int i = 0; i < numVals; i++) {
            vars[i] = model.newIntVar(0, numVals, "blah");
        }

        // Create the constraints.
        model.addAllDifferent(vars);
        for (int i = 0; i < numVals - 1; i++) {
            model.addLessOrEqual(vars[i], vars[i + 1]);
        }
        model.minimize(LinearExpr.sum(vars));
        model.addDecisionStrategy(vars, DecisionStrategyProto.VariableSelectionStrategy.CHOOSE_LOWEST_MIN,
                DecisionStrategyProto.getDefaultInstance().getDomainReductionStrategy());

        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setSearchBranching(SatParameters.SearchBranching.FIXED_SEARCH);
        solver.getParameters().setNumSearchWorkers(1);
        solver.getParameters().setStopAfterFirstSolution(true);
        solver.getParameters().setLogSearchProgress(true);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE ||
            status == CpSolverStatus.OPTIMAL) {
            for (int i = 0; i < numVals; i++) {
                System.out.println(String.format("blah[%s] = %s", i, solver.value(vars[i])));
            }
        }
        System.out.println(solver.responseStats());
    }

    @Test
    public void test2() {
        final long now = System.currentTimeMillis();
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final int numPods = 100;
        final int numNodes = 100;
        final IntVar[] podsControllableNodes = new IntVar[numPods];
        final int[] podsDemands = new int[numPods];

        for (int i = 0; i < numPods; i++) {
            podsControllableNodes[i] = model.newIntVar(0, numNodes - 1, "");
        }
        for (int i = 0; i < numPods; i++) {
            podsDemands[i] = 5;
        }

        // 1. Symmetry breaking
        for (int i = 0; i < numPods - 1; i++) {
            model.addLessThan(podsControllableNodes[i], podsControllableNodes[i + 1]);
        }

        // 2. Capacity constraint
        final IntVar[] loads = new IntVar[numNodes];
        final Map<Integer, IntVar[]> nodeToBools = new HashMap<>();
        for (int node = 0; node < numNodes; node++) {
            final IntVar[] bools = new IntVar[numPods];
            for (int i = 0; i < numPods; i++) {
                final IntVar bVar = model.newBoolVar("");
                model.addEquality(podsControllableNodes[i], node).onlyEnforceIf(bVar);
                model.addDifferent(podsControllableNodes[i], node).onlyEnforceIf(bVar.not());
                bools[i] = bVar;
            }
            final IntVar load = model.newIntVar(0, 10000000, "");
            model.addEquality(load, LinearExpr.scalProd(bools, podsDemands));
            loads[node] = load;
            model.addLessOrEqual(load, 100000);
            nodeToBools.put(node, bools);
        }

        final IntVar max = model.newIntVar(0, 1000000000, "");
        model.addMaxEquality(max, loads);
        model.minimize(max);

        System.out.println("Model creation: " + (System.currentTimeMillis() - now));
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
//        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setCpModelPresolve(false);
        solver.getParameters().setCpModelProbingLevel(0);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE ||
                status == CpSolverStatus.OPTIMAL) {
//            System.out.println(solver.value(max));
        }
        System.out.println("Done: " + (System.currentTimeMillis() - now));
//        System.out.println(solver.responseStats());
    }

    @Test
    public void test3() {
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final IntVar var = model.newIntVar(0, 100, "");
        final IntVar result = model.newIntVar(0, 1000, "");
        model.addEquality(result, LinearExpr.scalProd(new IntVar[]{var}, new int[]{8}));

        model.maximize(var);
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE ||
                status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(var));
            System.out.println(solver.value(result));
        }
        System.out.println(solver.responseStats());
    }


    @Test
    public void test4() {
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final IntVar var = model.newIntVar(0, 50, "");
        final IntVar bool = model.newBoolVar("");
        model.addEquality(var, 50).onlyEnforceIf(bool);
        model.addDifferent(var, 50).onlyEnforceIf(bool.not());

        model.maximize(var);
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE ||
                status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(var));
            System.out.println(solver.value(bool));
        }
        System.out.println(solver.responseStats());
    }
}