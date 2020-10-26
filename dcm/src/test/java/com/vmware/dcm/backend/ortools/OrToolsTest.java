/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.DecisionStrategyProto;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import com.google.ortools.sat.SatParameters;
import com.google.ortools.util.Domain;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.backend.ortools.StringEncoding;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class OrToolsTest {

    static {
        final OrToolsSolver builder = new OrToolsSolver.Builder().build(); // causes or-tools library to be loaded
        System.out.println(builder);
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
                System.out.printf("var[%s] = %s%n", i, solver.value(vars[i]));
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
        final int numPods = 1000;
        final int numNodes = 50;
        final IntVar[] podsControllableNodes = new IntVar[numPods];
        final int[] podsDemands = new int[numPods];

        for (int i = 0; i < numPods; i++) {
            podsControllableNodes[i] = model.newIntVar(0, numNodes - 1, "");
        }
        for (int i = 0; i < numPods; i++) {
            podsDemands[i] = ThreadLocalRandom.current().nextInt(0, 100);
        }

        // 1. Symmetry breaking
        for (int i = 0; i < numPods - 1; i++) {
            model.addLessThan(podsControllableNodes[i], podsControllableNodes[i + 1]);
        }

        // 2. Capacity constraint
        final IntVar[] loads = new IntVar[numNodes];
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
        }
        final IntVar max = model.newIntVar(0, 1000000000, "");
        model.addMaxEquality(max, loads);
        model.minimize(max);

        System.out.println("Model creation: " + (System.currentTimeMillis() - now));
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setCpModelPresolve(false);
        solver.getParameters().setCpModelProbingLevel(0);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(max));
        }
        System.out.println("Done: " + (System.currentTimeMillis() - now));
    }

    @Test
    public void testSlack() {
        final long now = System.currentTimeMillis();
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final int numPods = 1000;
        final int numNodes = 50;
        final IntVar[] podsControllableNodes = new IntVar[numPods];
        final int[] podsDemands1 = new int[numPods];
        final int[] podsDemands2 = new int[numPods];
        final int[] podsDemands3 = new int[numPods];

        final int[] nodeCapacities1 = new int[numNodes];
        final int[] nodeCapacities2 = new int[numNodes];
        final int[] nodeCapacities3 = new int[numNodes];

        for (int i = 0; i < numPods; i++) {
            podsControllableNodes[i] = model.newIntVar(0, numNodes - 1, "");
        }
        for (int i = 0; i < numPods; i++) {
            podsDemands1[i] = 1;
            podsDemands2[i] = 2;
            podsDemands3[i] = 3;
        }
        for (int i = 0; i < numNodes; i++) {
            nodeCapacities1[i] = 500;
            nodeCapacities2[i] = 600;
            nodeCapacities3[i] = 700;
        }

        // 1. Symmetry breaking
        for (int i = 0; i < numPods - 1; i++) {
            model.addLessOrEqual(podsControllableNodes[i], podsControllableNodes[i + 1]);
        }

        // 2. Capacity constraint
        final IntVar[] slacks1 = new IntVar[numNodes];

        for (int node = 0; node < numNodes; node++) {
            final IntVar[] bools = new IntVar[numPods];
            for (int i = 0; i < numPods; i++) {
                final IntVar bVar = model.newBoolVar("");
                model.addEquality(podsControllableNodes[i], node).onlyEnforceIf(bVar);
                model.addDifferent(podsControllableNodes[i], node).onlyEnforceIf(bVar.not());
                bools[i] = bVar;
            }
            final IntVar load1 = model.newIntVar(0, 10000000, "");
            final IntVar load2 = model.newIntVar(0, 10000000, "");
            final IntVar load3 = model.newIntVar(0, 10000000, "");
            model.addEquality(load1, LinearExpr.scalProd(bools, podsDemands1));
            model.addEquality(load2, LinearExpr.scalProd(bools, podsDemands2));
            model.addEquality(load3, LinearExpr.scalProd(bools, podsDemands3));

            final IntVar slack1 = model.newIntVar(0, 10000000, "");
            final IntVar slack2 = model.newIntVar(0, 10000000, "");
            final IntVar slack3 = model.newIntVar(0, 10000000, "");

            model.addEquality(slack1, LinearExpr.scalProd(new IntVar[]{model.newConstant(nodeCapacities1[node]), load1},
                                                                      new int[]{1, -1}));
            model.addEquality(slack2, LinearExpr.scalProd(new IntVar[]{model.newConstant(nodeCapacities2[node]), load2},
                    new int[]{1, -1}));
            model.addEquality(slack3, LinearExpr.scalProd(new IntVar[]{model.newConstant(nodeCapacities3[node]), load3},
                    new int[]{1, -1}));

            slacks1[node] = slack1;

            model.addGreaterOrEqual(slack1, 0);
            model.addGreaterOrEqual(slack2, 0);
            model.addGreaterOrEqual(slack3, 0);

        }
        final IntVar min1 = model.newIntVar(0, 1000000000, "");
        model.addMinEquality(min1, slacks1);
        model.maximize(min1);
        System.out.println("Model creation: " + (System.currentTimeMillis() - now));

        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setCpModelProbingLevel(0);

        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(min1));
        }
        System.out.println("Done: " + (System.currentTimeMillis() - now));
    }

    @Test
    public void test2Ineff() {
        final long now = System.currentTimeMillis();
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final int numPods = 100;
        final int numNodes = 1000;
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
        }
        final IntVar max = model.newIntVar(0, 1000000000, "");
        model.addMaxEquality(max, loads);
        model.minimize(max);

        System.out.println("Model creation: " + (System.currentTimeMillis() - now));
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setCpModelPresolve(false);
        solver.getParameters().setCpModelProbingLevel(0);
        final CpSolverStatus status = solver.solve(model);

        System.out.println(model.model().getConstraintsCount());
        System.out.println(model.model().getVariablesCount());
        if (status == CpSolverStatus.FEASIBLE ||
                status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(max));
        }
        System.out.println("Done: " + (System.currentTimeMillis() - now));
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
        final IntVar var1 = model.newIntVar(0, 100, "");
        final IntVar var2 = model.newIntVar(0, 100, "");
        final IntVar result = model.newIntVar(0, 1000, "");
        model.addProductEquality(result, new IntVar[]{var1, var2});

        model.maximize(result);
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE ||
                status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(var1));
            System.out.println(solver.value(var2));
            System.out.println(solver.value(result));
        }
        System.out.println(solver.responseStats());
    }


    @Test
    public void test5() {
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


    @Test
    public void test6() {
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final IntVar index = model.newIntVar(0, 4, "");
        final IntVar var1 = model.newIntVar(0, 100, "");
        final int[] var2 = new int[]{17, 41, 43, 93, 81};
        model.addElement(index, var2, var1);
        model.maximize(var1);
        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE ||
                status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(var1));
        }
        System.out.println(solver.responseStats());
    }

    @Test
    public void testStringEncoder() {
        final StringEncoding encoder = new StringEncoding();
        final long hello = encoder.toLong("hello");
        final long world = encoder.toLong("world");
        assertNotEquals(hello, world);

        final long helloAgain = encoder.toLong("hello");
        assertEquals(hello, helloAgain);

        final long worldAgain = encoder.toLong("world");
        assertEquals(world, worldAgain);

        assertEquals("hello", encoder.toStr(hello));
        assertEquals("hello", encoder.toStr(helloAgain));
        assertEquals("world", encoder.toStr(world));
        assertEquals("world", encoder.toStr(worldAgain));
    }



    @Test
    public void testDisjunctionWithMembership() {
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final IntVar var = model.newIntVar(0, 4, "");
        final IntVar bool = model.newBoolVar("");

        model.addLinearExpressionInDomain(var, Domain.fromValues(new long[]{1, 2, 3, 4})).onlyEnforceIf(bool);
        model.addDifferent(var, 1).onlyEnforceIf(bool.not());
        model.addDifferent(var, 2).onlyEnforceIf(bool.not());
        model.addDifferent(var, 3).onlyEnforceIf(bool.not());
        model.addDifferent(var, 4).onlyEnforceIf(bool.not());
        model.addEquality(bool, 0);

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