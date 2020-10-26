/*
 * Copyright 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.primitives.Ints;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.IntervalVar;
import com.google.ortools.sat.LinearExpr;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class OrToolsIntervalsTest {

    static {
        // causes or-tools library to be loaded
        final OrToolsSolver builder = new OrToolsSolver.Builder().build();
        System.out.println(builder);
    }

    @Test
    public void testWithIntervals() {
        final long now = System.currentTimeMillis();
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final int numTasks = 5;
        final int numNodes = 3;
        final IntVar[] taskToNodeAssignment = new IntVar[numTasks];
        final IntVar[] nodeIntervalEnd = new IntVar[numTasks];
        final IntervalVar[] tasksIntervals = new IntervalVar[numTasks];

        final int[] taskDemands1 = new int[numTasks];
        final int[] taskDemands2 = new int[numTasks];
        final int[] scores = new int[numTasks];

        final int[] nodeCapacities1 = new int[numNodes];
        final int[] nodeCapacities2 = new int[numNodes];

        for (int i = 0; i < numTasks; i++) {
            taskToNodeAssignment[i] = model.newIntVar(0, numNodes - 1, "");
            nodeIntervalEnd[i] = model.newIntVar(1, numNodes, "");

            // interval with start as taskToNodeAssignment and size of 1
            tasksIntervals[i] = model.newIntervalVar(taskToNodeAssignment[i],
                    model.newConstant(1), nodeIntervalEnd[i], "");
        }

        for (int i = 0; i < numNodes; i++) {
            nodeCapacities1[i] = ThreadLocalRandom.current().nextInt(400, 600);
            nodeCapacities2[i] = ThreadLocalRandom.current().nextInt(400, 600);
        }
        final int maxCapacity1 = Ints.max(nodeCapacities1);
        final int maxCapacity2 = Ints.max(nodeCapacities2);

        for (int i = 0; i < numTasks; i++) {
            if (i == 0) {
                taskDemands1[i] = 60;
                taskDemands2[i] = 50;
            } else {
                taskDemands1[i] = (ThreadLocalRandom.current().nextInt(10, 50) * 100) / maxCapacity1;
                taskDemands2[i] = (ThreadLocalRandom.current().nextInt(10, 50) * 100) / maxCapacity2;
            }
            scores[i] = taskDemands1[i] + taskDemands2[i];
        }

        // 1. Symmetry breaking
        for (int i = 0; i < numTasks - 1; i++) {
            model.addLessOrEqual(taskToNodeAssignment[i], taskToNodeAssignment[i + 1]);
        }

        // 2. Capacity constraints
        model.addCumulative(tasksIntervals, taskDemands1, model.newConstant(maxCapacity1));
        model.addCumulative(tasksIntervals, taskDemands2, model.newConstant(maxCapacity2));

        // Cumulative score
        final IntVar max1 = model.newIntVar(0, 10000000, "");
        model.addCumulative(tasksIntervals, scores, max1);
        model.minimize(max1);   // minimize max score
        System.out.println("Model creation: " + (System.currentTimeMillis() - now));

        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setCpModelProbingLevel(0);

        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(max1));
            System.out.println(Arrays.toString(taskDemands1));
            System.out.println(Arrays.toString(taskDemands2));
            for (int i = 0; i < numNodes; i++) {
                int sum = 0;
                for (int j = 0; j < numTasks; j++) {
                    if (solver.value(taskToNodeAssignment[j]) == i) {
                        sum += scores[j];
                        System.out.printf("Node[%s] has task[%s]%n", i, j);
                    }
                }
                System.out.printf("Node[%s] has score=%s%n", i, sum);
            }
        }
        System.out.println("Done: " + (System.currentTimeMillis() - now));
    }

    @Test
    public void testWithoutIntervals() {
        final long now = System.currentTimeMillis();
        // Create the model.
        final CpModel model = new CpModel();

        // Create the variables.
        final int numTasks = 50;
        final int numNodes = 100;
        final IntVar[] taskToNodeAssignment = new IntVar[numTasks];
        final int[] taskDemands1 = new int[numTasks];
        final int[] taskDemands2 = new int[numTasks];
        final int[] scores = new int[numTasks];

        final int[] nodeCapacities1 = new int[numNodes];
        final int[] nodeCapacities2 = new int[numNodes];

        for (int i = 0; i < numTasks; i++) {
            taskToNodeAssignment[i] = model.newIntVar(0, numNodes - 1, "");
        }
        for (int i = 0; i < numTasks; i++) {
            taskDemands1[i] = ThreadLocalRandom.current().nextInt(1, 5);
            taskDemands2[i] = ThreadLocalRandom.current().nextInt(1, 5);
            scores[i] = taskDemands1[i] + taskDemands2[i];
        }
        for (int i = 0; i < numNodes; i++) {
            nodeCapacities1[i] = 500;
            nodeCapacities2[i] = 600;
        }

        // 1. Symmetry breaking
        for (int i = 0; i < numTasks - 1; i++) {
            model.addLessOrEqual(taskToNodeAssignment[i], taskToNodeAssignment[i + 1]);
        }

        // 2. Capacity constraint
        final IntVar[] scoreVars = new IntVar[numNodes];
        for (int node = 0; node < numNodes; node++) {
            final IntVar[] tasksOnNode = new IntVar[numTasks];    // indicator whether task is assigned to this node
            for (int i = 0; i < numTasks; i++) {
                final IntVar bVar = model.newBoolVar("");
                model.addEquality(taskToNodeAssignment[i], node).onlyEnforceIf(bVar);
                model.addDifferent(taskToNodeAssignment[i], node).onlyEnforceIf(bVar.not());
                tasksOnNode[i] = bVar;
            }
            final IntVar load1 = model.newIntVar(0, 10000000, "");
            final IntVar load2 = model.newIntVar(0, 10000000, "");
            final IntVar score = model.newIntVar(0, 10000000, "");
            model.addEquality(load1, LinearExpr.scalProd(tasksOnNode, taskDemands1));  // cpu load = sum of all tasks
            model.addEquality(load2, LinearExpr.scalProd(tasksOnNode, taskDemands2));  // mem load variable
            model.addEquality(score, LinearExpr.scalProd(tasksOnNode, scores)); //score variable for this node

            scoreVars[node] = score;

            model.addLessOrEqual(load1, nodeCapacities1[node]);  // capacity constraints
            model.addLessOrEqual(load2, nodeCapacities2[node]);
        }

        final IntVar max1 = model.newIntVar(0, 10000000, "");
        model.addMaxEquality(max1, scoreVars);
        model.minimize(max1);   // minimize max score
        System.out.println("Model creation: " + (System.currentTimeMillis() - now));

        // Create a solver and solve the model.
        final CpSolver solver = new CpSolver();
        solver.getParameters().setNumSearchWorkers(4);
        solver.getParameters().setLogSearchProgress(true);
        solver.getParameters().setCpModelProbingLevel(0);

        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL) {
            System.out.println(solver.value(max1));
        }
        System.out.println("Done: " + (System.currentTimeMillis() - now));
    }
}