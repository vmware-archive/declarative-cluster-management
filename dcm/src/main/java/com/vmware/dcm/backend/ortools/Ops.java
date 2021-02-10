/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.IntervalVar;
import com.google.ortools.sat.LinearExpr;
import com.google.ortools.sat.Literal;
import com.google.ortools.util.Domain;
import com.vmware.dcm.SolverException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Ops {
    private static final long DOMAIN_MIN = Integer.MIN_VALUE;
    private static final long DOMAIN_MAX = Integer.MAX_VALUE;
    private final CpModel model;
    private final StringEncoding encoder;
    private final IntVar trueVar;
    private final IntVar falseVar;


    public Ops(final CpModel model, final StringEncoding encoding) {
        this.model = model;
        this.encoder = encoding;
        this.trueVar = model.newConstant(1);
        this.falseVar = model.newConstant(0);
    }

    public int sumInteger(final List<Integer> data) {
        int ret = 0;
        for (final Integer d: data) {
            ret += d;
        }
        return ret;
    }

    public long sumLong(final List<Long> data) {
        long ret = 0;
        for (final long d: data) {
            ret += d;
        }
        return ret;
    }

    public IntVar sumV(final List<IntVar> data) {
        long domainMin = 0;
        long domainMax = 0;
        final IntVar[] arr = new IntVar[data.size()];
        for (int i = 0; i < data.size(); i++) {
            domainMin += data.get(i).getDomain().min();
            domainMax += data.get(i).getDomain().max();
            arr[i] = data.get(i);
        }
        final IntVar ret = model.newIntVar(domainMin, domainMax, "");
        model.addEquality(ret, LinearExpr.sum(arr));
        return ret;
    }

    // TODO: add test case to OpsTests
    public IntVar scalProdLong(final List<IntVar> variables, final List<Long> coefficients) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.scalProd(variables.toArray(new IntVar[0]),
                coefficients.stream().mapToLong(Long::longValue).toArray()));
        return ret;
    }

    public IntVar scalProdInteger(final List<IntVar> variables, final List<Integer> coefficients) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.scalProd(variables.toArray(new IntVar[0]),
                                                   coefficients.stream().mapToInt(Integer::intValue).toArray()));
        return ret;
    }

    public void increasing(final List<IntVar> data) {
        for (int i = 0; i < data.size() - 1; i++) {
            model.addLessOrEqual(data.get(i), data.get(i + 1));

            final IntVar bool = model.newBoolVar("");
            model.addLessThan(data.get(i), data.get(i + 1)).onlyEnforceIf(bool); // soft constraint to maximize
            model.maximize(LinearExpr.term(bool, 100));
        }
    }

    public IntVar exists(final List<IntVar> data) {
        final IntVar bool = model.newBoolVar("");
        final Literal[] literals = data.toArray(new Literal[0]);
        model.addBoolOr(literals).onlyEnforceIf(bool);
        model.addBoolAnd(data.stream().map(IntVar::not).toArray(Literal[]::new)).onlyEnforceIf(bool.not());
        return bool;
    }

    public int maxVInteger(final List<Integer> data) {
        return Collections.max(data);
    }

    public long maxVLong(final List<Long> data) {
        return Collections.max(data);
    }

    public IntVar maxVIntVar(final List<IntVar> data) {
        long domainMin = Long.MIN_VALUE;
        long domainMax = Long.MIN_VALUE;
        final IntVar[] arr = new IntVar[data.size()];
        for (int i = 0; i < data.size(); i++) {
            domainMin = Math.max(domainMin, data.get(i).getDomain().min());
            domainMax = Math.max(domainMax, data.get(i).getDomain().max());
            arr[i] = data.get(i);
        }
        final IntVar ret = model.newIntVar(domainMin, domainMax, "");
        model.addMaxEquality(ret, arr);
        return ret;
    }

    public int minVInteger(final List<Integer> data) {
        return Collections.min(data);
    }

    public long minVLong(final List<Long> data) {
        return Collections.min(data);
    }

    public IntVar minVIntVar(final List<IntVar> data) {
        long domainMin = Long.MAX_VALUE;
        long domainMax = Long.MAX_VALUE;
        final IntVar[] arr = new IntVar[data.size()];
        for (int i = 0; i < data.size(); i++) {
            domainMin = Math.min(domainMin, data.get(i).getDomain().min());
            domainMax = Math.min(domainMax, data.get(i).getDomain().max());
            arr[i] = data.get(i);
        }
        final IntVar ret = model.newIntVar(domainMin, domainMax, "");
        model.addMinEquality(ret, arr);
        return ret;
    }

    public int countV(final long[] data) {
        return data.length;
    }

    public IntVar div(final IntVar left, final int right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addDivisionEquality(ret, left, model.newConstant(right));
        return ret;
    }

    public IntVar plus(final int left, final IntVar right) {
        return plus(model.newConstant(left), right);
    }

    public IntVar plus(final IntVar left, final int right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, model.newConstant(right)}));
        return ret;
    }

    public IntVar plus(final IntVar left, final IntVar right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, right}));
        return ret;
    }

    public IntVar minus(final int left, final IntVar right) {
        return minus(model.newConstant(left), right);
    }

    public IntVar minus(final IntVar left, final int right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, model.newConstant(-right)}));
        return ret;
    }

    public IntVar minus(final IntVar left, final IntVar right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.scalProd(new IntVar[]{left, right}, new int[]{1, -1}));
        return ret;
    }

    public int mult(final int left, final int right) {
        return left * right;
    }

    public IntVar mult(final int left, final IntVar right) {
        return mult(right, left);
    }

    public IntVar mult(final IntVar left, final int right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addEquality(ret, LinearExpr.term(left, right));
        return ret;
    }

    public IntVar mult(final IntVar left, final IntVar right) {
        final IntVar ret = model.newIntVar(DOMAIN_MIN, DOMAIN_MAX, "");
        model.addProductEquality(ret, new IntVar[]{left, right});
        return ret;
    }

    public boolean eq(final boolean left, final boolean right) {
        return right == left;
    }

    public boolean eq(final String left, final String right) {
        return right.equals(left);
    }

    public boolean eq(final int left, final int right) {
        return left == right;
    }

    public boolean eq(final long left, final long right) {
        return left == right;
    }

    public IntVar eq(final String left, final IntVar right) {
        return eq(right, left);
    }

    public IntVar eq(final IntVar left, final String right) {
        return eq(left, encoder.toLong(right));
    }

    public IntVar eq(final long left, final IntVar right) {
        return eq(right, left);
    }

    public IntVar eq(final IntVar left, final long right) {
        final IntVar bool = model.newBoolVar("");
        model.addEquality(left, right).onlyEnforceIf(bool);
        model.addDifferent(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar eq(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addEquality(left, right).onlyEnforceIf(bool);
        model.addDifferent(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar eq(final IntVar left, final boolean right) {
        return eq(left, right ? 1 : 0);
    }

    public IntVar eq(final boolean left, final IntVar right) {
        return eq(right, left);
    }

    public boolean ne(final boolean left, final boolean right) {
        return right != left;
    }

    public boolean ne(final String left, final String right) {
        return !right.equals(left);
    }

    public boolean ne(final int left, final int right) {
        return left != right;
    }

    public boolean ne(final long left, final long right) {
        return left != right;
    }

    public IntVar ne(final String left, final IntVar right) {
        return ne(right, left);
    }

    public IntVar ne(final IntVar left, final String right) {
        return ne(left, encoder.toLong(right));
    }

    public IntVar ne(final long left, final IntVar right) {
        return ne(right, left);
    }

    public IntVar ne(final IntVar left, final long right) {
        final IntVar bool = model.newBoolVar("");
        model.addDifferent(left, right).onlyEnforceIf(bool);
        model.addEquality(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar ne(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addDifferent(left, right).onlyEnforceIf(bool);
        model.addEquality(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar ne(final boolean left, final IntVar right) {
        return ne(right, left);
    }

    public IntVar ne(final IntVar left, final boolean right) {
        return ne(left, right ? 1 : 0);
    }

    public IntVar lt(final IntVar left, final long right) {
        final IntVar bool = model.newBoolVar("");
        model.addLessThan(left, right).onlyEnforceIf(bool);
        model.addGreaterOrEqual(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar lt(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addLessThan(left, right).onlyEnforceIf(bool);
        model.addGreaterOrEqual(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar leq(final IntVar left, final long right) {
        final IntVar bool = model.newBoolVar("");
        model.addLessOrEqual(left, right).onlyEnforceIf(bool);
        model.addGreaterThan(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar leq(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addLessOrEqual(left, right).onlyEnforceIf(bool);
        model.addGreaterThan(left, right).onlyEnforceIf(bool.not());
        return bool;
    }


    public IntVar gt(final IntVar left, final long right) {
        final IntVar bool = model.newBoolVar("");
        model.addGreaterThan(left, right).onlyEnforceIf(bool);
        model.addLessOrEqual(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar gt(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addGreaterThan(left, right).onlyEnforceIf(bool);
        model.addLessOrEqual(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar geq(final IntVar left, final long right) {
        final IntVar bool = model.newBoolVar("");
        model.addGreaterOrEqual(left, right).onlyEnforceIf(bool);
        model.addLessThan(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar geq(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addGreaterOrEqual(left, right).onlyEnforceIf(bool);
        model.addLessThan(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public boolean in(final String left, final Object[] right) {
        assert right.length > 0 && right[0] instanceof String;
        for (final Object obj: right) {
            if (left.equals(obj)) {
                return true;
            }
        }
        return false;
    }

    public boolean in(final String left, final List<String> right) {
        return right.contains(left);
    }

    public boolean in(final int left, final List<Integer> right) {
        return right.contains(left);
    }

    public boolean in(final long left, final List<Long> right) {
        return right.contains(left);
    }

    public IntVar inObjectArr(final IntVar left, final Object[] right) {
        final IntVar bool = model.newBoolVar("");
        assert right.length > 0;
        final Domain domain;
        if (right[0] instanceof String) {
            domain = Domain.fromValues(Arrays.stream(right)
                    .map(e -> (String) e)
                    .mapToLong(encoder::toLong).toArray());
        } else if (right[0] instanceof Integer) {
            domain = Domain.fromValues(Arrays.stream(right)
                    .map(e -> (Integer) e)
                    .mapToLong(encoder::toLong).toArray());
        } else {
            throw new RuntimeException("Unexpected object array " + Arrays.toString(right));
        }
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inString(final IntVar left, final List<String> right) {
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inLong(final IntVar left, final List<Long> right) {
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inInteger(final IntVar left, final List<Integer> right) {
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inIntVar(final IntVar left, final List<IntVar> right) {
        final IntVar bool = model.newBoolVar("");
        final Literal[] literals = new Literal[right.size()];
        for (int i = 0; i < right.size(); i++) {
            literals[i] = eq(left, right.get(i));
        }
        model.addBoolOr(literals).onlyEnforceIf(bool);

        for (int i = 0; i < right.size(); i++) {
            literals[i] = literals[i].not();
        }
        model.addBoolAnd(literals).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar or(final boolean left, final IntVar right) {
        return left ? trueVar : right;
    }

    public IntVar or(final IntVar left, final boolean right) {
        return or(right, left);
    }

    public IntVar or(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addBoolOr(new Literal[]{left, right}).onlyEnforceIf(bool);
        model.addBoolAnd(new Literal[]{left.not(), right.not()}).onlyEnforceIf(bool.not());
        return bool;
    }


    public IntVar and(final boolean left, final IntVar right) {
        return left ? right : falseVar;
    }

    public IntVar and(final IntVar left, final boolean right) {
        return and(right, left);
    }

    public IntVar and(final IntVar left, final IntVar right) {
        final IntVar bool = model.newBoolVar("");
        model.addBoolAnd(new Literal[]{left, right}).onlyEnforceIf(bool);
        model.addBoolOr(new Literal[]{left.not(), right.not()}).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar not(final IntVar var) {
        return eq(var, 0L);
    }

    public boolean not(final boolean var) {
        return !var;
    }

    public <T> boolean allEqualPrimitive(final List<T> array) {
        for (int i = 0; i < array.size() - 1; i++) {
            if (array.get(i) != array.get(i + 1)) {
                return false;
            }
        }
        return true;
    }

    public void allEqualVar(final List<IntVar> array) {
        for (int i = 0; i < array.size() - 1; i++) {
            model.addEquality(array.get(i), array.get(i + 1));
        }
    }

    public <T> void allDifferent(final List<IntVar> array) {
        final IntVar[] intVars = array.toArray(new IntVar[0]);
        model.addAllDifferent(intVars);
    }

    public IntVar toConst(final boolean expr) {
        return expr ? trueVar : falseVar;
    }

    public IntVar toConst(final long expr) {
        return model.newConstant(expr);
    }

    public void capacityConstraint(final List<IntVar> varsToAssign, final List<?> domain,
                                   final List<List<Long>> demands, final List<List<Long>> capacities) {
        // Create the variables.
        capacities.forEach(
                vec -> {
                    Preconditions.checkArgument(domain.size() == vec.size(),
                            "Capacities and domain vectors are of different sizes");
                    Preconditions.checkArgument(vec.stream().allMatch(capacity -> capacity >= 0),
                            "Negative values for capacities are not allowed");
                }
        );
        demands.forEach(
                vec -> {
                    Preconditions.checkArgument(varsToAssign.size() == vec.size(),
                            "Capacities and domain vectors are of different sizes");
                    Preconditions.checkArgument(vec.stream().allMatch(demand -> demand >= 0),
                            "Negative values for demands are not allowed");
                }
        );
        if (domain.size() == 0) {
            // Providing an empty domain to a set of vars is trivially false.
            throw new SolverException("Empty domain for capacity constraint " + demands + " " + capacities);
        }

        if (domain.get(0) instanceof String) {
            final long[] domainArr = domain.stream().mapToLong(o -> encoder.toLong((String) o)).toArray();
            capacityConstraint(varsToAssign, domainArr, demands, capacities);
        } else if (domain.get(0) instanceof Integer) {
            final long[] domainArr = domain.stream().mapToLong(o -> encoder.toLong((Integer) o)).toArray();
            capacityConstraint(varsToAssign, domainArr, demands, capacities);
        } else if (domain.get(0) instanceof Long) {
            final long[] domainArr = domain.stream().mapToLong(o -> encoder.toLong((Long) o)).toArray();
            capacityConstraint(varsToAssign, domainArr, demands, capacities);
        } else {
            // Keep this a runtime exception because this can only happen if the compiler
            // did not correctly type check
            throw new RuntimeException("Unexpected type of list: " + domain);
        }
    }

    public void capacityConstraint(final List<IntVar> varsToAssign, final long[] domainArr,
                                   final List<List<Long>> demands, final List<List<Long>> capacities) {
        Preconditions.checkArgument(demands.size() == capacities.size());

        final IntVar[] taskToNodeAssignment = varsToAssign.toArray(IntVar[]::new);
        final int numTasks = taskToNodeAssignment.length;
        final IntervalVar[] tasksIntervals = new IntervalVar[numTasks + capacities.get(0).size()];

        final Domain domainT = Domain.fromValues(domainArr);
        final Domain intervalRange = Domain.fromFlatIntervals(new long[] {domainT.min() + 1, domainT.max() + 1});
        for (int i = 0; i < numTasks; i++) {
            model.addLinearExpressionInDomain(taskToNodeAssignment[i], domainT);
            final IntVar intervalEnd = model.newIntVarFromDomain(intervalRange, "");

            // interval with start as taskToNodeAssignment and size of 1
            tasksIntervals[i] = model.newIntervalVar(taskToNodeAssignment[i],
                    model.newConstant(1), intervalEnd, "");
        }

        // Create dummy intervals
        for (int i = numTasks; i < tasksIntervals.length; i++) {
            final int nodeIndex = i - numTasks;
            tasksIntervals[i] = model.newFixedInterval(domainArr[nodeIndex], 1, "");
        }

        // Convert to list of arrays
        final List<long[]> nodeCapacities =
                capacities.stream().map(vec -> vec.stream().mapToLong(Long::longValue).toArray())
                        .collect(Collectors.toList());
        final List<Long> maxCapacities = nodeCapacities.stream().map(la -> Arrays.stream(la).max().getAsLong())
                .collect(Collectors.toList());
        final int numResources = demands.size();

        // For each resource, create dummy demands to accommodate heterogeneous capacities
        final List<long[]> updatedDemands = new ArrayList<>(demands.size());
        for (int i = 0; i < numResources; i++) {
            final List<Long> demand = new ArrayList<>(demands.get(i));
            final long maxCapacity = maxCapacities.get(i);
            for (final long value : nodeCapacities.get(i)) {
                demand.add(maxCapacity - value);
            }
            updatedDemands.add(demand.stream().mapToLong(Long::longValue).toArray());
        }
        updatedDemands.forEach(
                vec -> Preconditions.checkArgument(vec.length == (numTasks + capacities.get(0).size()))
        );

        // 2. Capacity constraints
        for (int i = 0; i < numResources; i++) {
            model.addCumulative(tasksIntervals, updatedDemands.get(i), maxCapacities.get(i));
        }

        // Cumulative score
        for (int i = 0; i < numResources; i++) {
            final IntVar max = model.newIntVar(0, maxCapacities.get(i), "");
            model.addCumulative(tasksIntervals, updatedDemands.get(i), max);
            model.minimize(max);
        }
    }
}