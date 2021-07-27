/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.IntegerVariableProto;
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

    public int countBoolean(final List<Boolean> data) {
        return data.size();
    }

    public int countInteger(final List<Integer> data) {
        return data.size();
    }

    public int countLong(final List<Long> data) {
        return data.size();
    }

    public int countString(final List<String> data) {
        return data.size();
    }

    public IntVar countIntVar(final List<IntVar> data) {
        final List<IntVar> isZero = new ArrayList<>(data.size());
        for (final IntVar v: data) {
            isZero.add(ne(v, 0));
        }
        return sumIntVar(isZero);
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

    public IntVar sumIntVar(final List<IntVar> data) {
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

    public IntVar scalProdLong(final List<IntVar> variables, final List<Long> coefficients) {
        final int n = variables.size();
        Preconditions.checkArgument(n == coefficients.size());
        long minSum = 0;
        long maxSum = 0;
        final IntVar[] vars = new IntVar[n];
        final long[] coeffs = new long[n];
        for (int i = 0; i < n; i++) {
            final IntVar var = variables.get(i);
            final Domain domain = var.getDomain();
            final long coeff = coefficients.get(i);
            minSum += domain.min() * coeff;
            maxSum += domain.max() * coeff;
            vars[i] = var;
            coeffs[i] = coeff;
        }
        final IntVar ret = model.newIntVar(Math.min(minSum, maxSum), Math.max(minSum, maxSum), "");
        model.addEquality(ret, LinearExpr.scalProd(vars, coeffs));
        return ret;
    }

    public IntVar scalProdInteger(final List<IntVar> variables, final List<Integer> coefficients) {
        final int n = variables.size();
        Preconditions.checkArgument(n == coefficients.size());
        long minSum = 0;
        long maxSum = 0;
        final IntVar[] vars = new IntVar[n];
        final long[] coeffs = new long[n];
        for (int i = 0; i < n; i++) {
            final IntVar var = variables.get(i);
            final Domain domain = var.getDomain();
            final long coeff = coefficients.get(i);
            minSum += domain.min() * coeff;
            maxSum += domain.max() * coeff;
            vars[i] = var;
            coeffs[i] = coeff;
        }
        final IntVar ret = model.newIntVar(Math.min(minSum, maxSum), Math.max(minSum, maxSum), "");
        model.addEquality(ret, LinearExpr.scalProd(vars, coeffs));
        return ret;
    }

    public IntVar increasingIntVar(final List<IntVar> data) {
        for (int i = 0; i < data.size() - 1; i++) {
            model.addLessOrEqual(data.get(i), data.get(i + 1));

            final IntVar bool = model.newBoolVar("");
            model.addLessThan(data.get(i), data.get(i + 1)).onlyEnforceIf(bool); // soft constraint to maximize
            model.maximize(LinearExpr.term(bool, 100));
        }
        return model.newConstant(1);
    }

    public IntVar exists(final List<IntVar> data) {
        if (data.size() == 0) {
            return falseVar;
        }
        if (data.size() == 1) {
            return eq(data.get(0), true);
        }
        final IntVar bool = model.newBoolVar("");
        final Literal[] literals = data.toArray(new Literal[0]);
        model.addBoolOr(literals).onlyEnforceIf(bool);
        model.addBoolAnd(data.stream().map(IntVar::not).toArray(Literal[]::new)).onlyEnforceIf(bool.not());
        return bool;
    }

    public int maxInteger(final List<Integer> data) {
        return Collections.max(data);
    }

    public long maxLong(final List<Long> data) {
        return Collections.max(data);
    }

    public IntVar maxIntVar(final List<IntVar> data) {
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

    public int minInteger(final List<Integer> data) {
        return Collections.min(data);
    }

    public long minLong(final List<Long> data) {
        return Collections.min(data);
    }

    public IntVar minIntVar(final List<IntVar> data) {
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

    public IntVar div(final IntVar left, final int right) {
        final Domain domain = left.getDomain();
        final long minDivResult = domain.min() / right;
        final long maxDivResult = domain.max() / right;
        final long lb = Math.min(minDivResult, maxDivResult);
        final long ub = Math.max(minDivResult, maxDivResult);
        final IntVar ret = model.newIntVar(lb, ub, "");
        model.addDivisionEquality(ret, left, model.newConstant(right));
        return ret;
    }

    public IntVar plus(final int left, final IntVar right) {
        return plus(model.newConstant(left), right);
    }

    public IntVar plus(final IntVar left, final int right) {
        final Domain domain = left.getDomain();
        final IntVar ret = model.newIntVar(domain.min() + right, domain.max() + right, "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, model.newConstant(right)}));
        return ret;
    }

    public IntVar plus(final IntVar left, final IntVar right) {
        final Domain lDomain = left.getDomain();
        final Domain rDomain = right.getDomain();
        final IntVar ret = model.newIntVar(lDomain.min()  + rDomain.min(),
                                        lDomain.max()  + rDomain.max(), "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, right}));
        return ret;
    }

    public IntVar minus(final int left, final IntVar right) {
        return minus(model.newConstant(left), right);
    }

    public IntVar minus(final IntVar left, final int right) {
        final Domain domain = left.getDomain();
        final long leftLbResult = domain.min() - right;
        final long leftUbResult = domain.max() - right;
        final long lb = Math.min(leftLbResult, leftUbResult);
        final long ub = Math.max(leftLbResult, leftUbResult);
        final IntVar ret = model.newIntVar(lb, ub, "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, model.newConstant(-right)}));
        return ret;
    }

    public IntVar minus(final IntVar left, final IntVar right) {
        final Domain lDomain = left.getDomain();
        final Domain rDomain = right.getDomain();
        final long lDomainMin = lDomain.min();
        final long lDomainMax = lDomain.max();
        final long rDomainMin = rDomain.min();
        final long rDomainMax = rDomain.max();
        final long lb = Math.min(lDomainMin - rDomainMin, lDomainMin - rDomainMax);
        final long ub = Math.max(lDomainMax - rDomainMin, lDomainMax - rDomainMax);
        final IntVar ret = model.newIntVar(lb, ub, "");
        model.addEquality(ret, LinearExpr.scalProd(new IntVar[]{left, right}, new int[]{1, -1}));
        return ret;
    }

    public long mult(final long left, final long right) {
        return left * right;
    }

    public IntVar mult(final long left, final IntVar right) {
        return mult(right, left);
    }

    public int mult(final int left, final int right) {
        return left * right;
    }

    public IntVar mult(final int left, final IntVar right) {
        return mult(right, left);
    }

    public IntVar mult(final IntVar left, final long right) {
        if (right == 1) {
            return left;
        }
        if (right == 0) {
            return model.newConstant(0);
        }
        final Domain domain = left.getDomain();
        final long lDomainMinResult = domain.min() * right;
        final long lDomainMaxResult = domain.max() * right;

        // Conservative. Should be fixed with: https://github.com/vmware/declarative-cluster-management/issues/112
        final long lb = Math.min(lDomainMinResult, lDomainMaxResult);
        final long ub = Math.max(lDomainMinResult, lDomainMaxResult);
        final IntVar ret = model.newIntVar(lb, ub, "");
        model.addEquality(ret, LinearExpr.term(left, right));
        return ret;
    }

    public IntVar mult(final IntVar left, final IntVar right) {
        final Domain lDomain = left.getDomain();
        final Domain rDomain = right.getDomain();
        final long lDomainMin = lDomain.min();
        final long lDomainMax = lDomain.max();
        final long rDomainMin = rDomain.min();
        final long rDomainMax = rDomain.max();
        final long b1 = lDomainMin * rDomainMin;
        final long b2 = lDomainMax * rDomainMax;
        final long b3 = lDomainMax * rDomainMin;
        final long b4 = lDomainMin * rDomainMax;

        // Conservative. Should be fixed with: https://github.com/vmware/declarative-cluster-management/issues/112
        final long lb = Math.min(Math.min(b1, b2), Math.min(b3, b4));
        final long ub = Math.max(Math.max(b1, b2), Math.max(b3, b4));
        final IntVar ret = model.newIntVar(lb, ub, "");
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
        final Domain leftDomain = left.getDomain();
        final long leftDomainSize = leftDomain.size();
        final long leftDomainMin = leftDomain.min();
        if (leftDomainSize == 1) {
            return leftDomainMin == right ? trueVar : falseVar;
        }
        final IntVar bool = model.newBoolVar("");
        model.addEquality(left, right).onlyEnforceIf(bool);
        model.addDifferent(left, right).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar eq(final IntVar left, final IntVar right) {
        final Domain leftDomain = left.getDomain();
        final Domain rightDomain = right.getDomain();
        final long leftDomainSize = leftDomain.size();
        final long rightDomainSize = rightDomain.size();
        final long leftDomainMin = leftDomain.min();
        final long rightDomainMin = rightDomain.min();
        if (rightDomainSize == 1 && leftDomainSize == 1) {
            return leftDomainMin == rightDomainMin ? trueVar : falseVar;
        }
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
        if (right.size() == 0) {
            return falseVar;
        }
        if (right.size() == 1) {
            return eq(left, right.get(0));
        }
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inLong(final IntVar left, final List<Long> right) {
        if (right.size() == 0) {
            return falseVar;
        }
        if (right.size() == 1) {
            return eq(left, right.get(0));
        }
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inInteger(final IntVar left, final List<Integer> right) {
        if (right.size() == 0) {
            return falseVar;
        }
        if (right.size() == 1) {
            return eq(left, right.get(0));
        }
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain).onlyEnforceIf(bool);
        model.addLinearExpressionInDomain(left, domain.complement()).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar inIntVar(final IntVar left, final List<IntVar> right) {
        if (right.size() == 0) {
            return falseVar;
        }
        if (right.size() == 1) {
            return eq(left, right.get(0));
        }
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

    public IntVar inObjectArray(final IntVar left, final List<Object[]> right) {
        if (right.size() == 0) {
            return falseVar;
        }
        if (right.size() == 1) {
            return inObjectArr(left, right.get(0));
        }
        final IntVar bool = model.newBoolVar("");
        final Literal[] literals = new Literal[right.size()];
        for (int i = 0; i < right.size(); i++) {
            literals[i] = inObjectArr(left, right.get(i));
        }
        model.addBoolOr(literals).onlyEnforceIf(bool);

        for (int i = 0; i < right.size(); i++) {
            literals[i] = literals[i].not();
        }
        model.addBoolAnd(literals).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar or(final boolean left, final boolean right) {
        return left || right ? trueVar : falseVar;
    }

    public IntVar or(final boolean left, final IntVar right) {
        return left ? trueVar : right;
    }

    public IntVar or(final IntVar left, final boolean right) {
        return or(right, left);
    }

    public IntVar or(final IntVar left, final IntVar right) {
        final Domain leftDomain = left.getDomain();
        final Domain rightDomain = right.getDomain();
        final long leftDomainSize = leftDomain.size();
        final long rightDomainSize = rightDomain.size();
        final long leftDomainMin = leftDomain.min();
        final long rightDomainMin = rightDomain.min();
        if (rightDomainSize == 1 && leftDomainSize == 1) {
            return or(leftDomainMin != 0, rightDomainMin != 0);
        }
        if (leftDomainSize == 1) {
            return or(leftDomainMin != 0, right);
        }
        if (rightDomainSize == 1) {
            return or(left, rightDomainMin != 0);
        }
        final IntVar bool = model.newBoolVar("");
        model.addBoolOr(new Literal[]{left, right}).onlyEnforceIf(bool);
        model.addBoolAnd(new Literal[]{left.not(), right.not()}).onlyEnforceIf(bool.not());
        return bool;
    }

    public IntVar and(final boolean left, final boolean right) {
        return left && right ? trueVar : falseVar;
    }

    public IntVar and(final boolean left, final IntVar right) {
        return left ? right : falseVar;
    }

    public IntVar and(final IntVar left, final boolean right) {
        return and(right, left);
    }

    public IntVar and(final IntVar left, final IntVar right) {
        final Domain leftDomain = left.getDomain();
        final Domain rightDomain = right.getDomain();
        final long leftDomainSize = leftDomain.size();
        final long rightDomainSize = rightDomain.size();
        final long leftDomainMin = leftDomain.min();
        final long rightDomainMin = rightDomain.min();
        if (rightDomainSize == 1 && leftDomainSize == 1) {
            return and(leftDomainMin != 0, rightDomainMin != 0);
        }
        if (leftDomainSize == 1) {
            return and(leftDomainMin != 0, right);
        }
        if (rightDomainSize == 1) {
            return and(left, rightDomainMin != 0);
        }
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

    public boolean anyBoolean(final List<Boolean> array) {
        if (array.size() == 0) {
            throw new SolverException("Empty list for aggregate function any()");
        }
        return array.stream().reduce((a, b) -> a || b).get();
    }

    public IntVar anyIntVar(final List<IntVar> array) {
        if (array.size() == 0) {
            throw new SolverException("Empty list for aggregate function any()");
        }
        final IntVar res = model.newBoolVar("");
        final Literal[] literals = new Literal[array.size()];
        final Literal[] negatedLiterals = new Literal[array.size()];
        for (int i = 0; i < array.size(); i++) {
            final IntVar var = array.get(i);
            literals[i] = var;
            negatedLiterals[i] = var.not();
        }
        model.addBoolOr(literals).onlyEnforceIf(res);
        model.addBoolAnd(negatedLiterals).onlyEnforceIf(res.not());
        return res;
    }

    public boolean allBoolean(final List<Boolean> array) {
        if (array.size() == 0) {
            throw new SolverException("Empty list for aggregate function all()");
        }
        return array.stream().reduce((a, b) -> a && b).get();
    }

    public IntVar allIntVar(final List<IntVar> array) {
        if (array.size() == 0) {
            throw new SolverException("Empty list for aggregate function all()");
        }
        final IntVar res = model.newBoolVar("");
        final Literal[] literals = new Literal[array.size()];
        final Literal[] negatedLiterals = new Literal[array.size()];
        for (int i = 0; i < array.size(); i++) {
            final IntVar var = array.get(i);
            literals[i] = var;
            negatedLiterals[i] = var.not();
        }
        model.addBoolAnd(literals).onlyEnforceIf(res);
        model.addBoolOr(negatedLiterals).onlyEnforceIf(res.not());
        return res;
    }

    public boolean allEqualInteger(final List<Integer> array) {
        return allEqualPrimitive(array);
    }

    public boolean allEqualString(final List<String> array) {
        return allEqualPrimitive(array);
    }

    public boolean allEqualLong(final List<Long> array) {
        return allEqualPrimitive(array);
    }

    public boolean allEqualBoolean(final List<Boolean> array) {
        return allEqualPrimitive(array);
    }

    private  <T> boolean allEqualPrimitive(final List<T> array) {
        for (int i = 0; i < array.size() - 1; i++) {
            if (!array.get(i).equals(array.get(i + 1))) {
                return false;
            }
        }
        return true;
    }

    public IntVar allEqualIntVar(final List<IntVar> array) {
        for (int i = 0; i < array.size() - 1; i++) {
            model.addEquality(array.get(i), array.get(i + 1));
        }
        return model.newConstant(1);
    }

    public <T> IntVar allDifferentIntVar(final List<IntVar> array, final String assumptionContext) {
        final IntVar[] intVars = array.toArray(new IntVar[0]);
        final IntVar[] assumptionVars = assumptionLinkedVars(intVars, assumptionContext);
        model.addAllDifferent(assumptionVars);
        return model.newConstant(1);
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
            final long[] domainArr = domain.stream().mapToLong(o -> encoder.toLong((int) o)).toArray();
            capacityConstraint(varsToAssign, domainArr, demands, capacities);
        } else if (domain.get(0) instanceof Long) {
            final long[] domainArr = domain.stream().mapToLong(o -> encoder.toLong((long) o)).toArray();
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

        final int numTasks = varsToAssign.size();
        final int numResources = demands.size();
        final IntervalVar[] tasksIntervals = new IntervalVar[numTasks + capacities.get(0).size()];

        final Domain domainT = Domain.fromValues(domainArr);
        final Domain intervalRange = Domain.fromFlatIntervals(new long[] {domainT.min() + 1, domainT.max() + 1});
        final IntVar unitIntervalSize = model.newConstant(1);
        for (int i = 0; i < numTasks; i++) {
            final IntVar intervalEnd = model.newIntVarFromDomain(intervalRange, "");

            // interval with start as taskToNodeAssignment and size of 1
            tasksIntervals[i] = model.newIntervalVar(varsToAssign.get(i), unitIntervalSize, intervalEnd, "");
        }

        // Create dummy intervals
        for (int i = numTasks; i < tasksIntervals.length; i++) {
            final int nodeIndex = i - numTasks;
            tasksIntervals[i] = model.newFixedInterval(domainArr[nodeIndex], 1, "");
        }

        // Convert to list of arrays
        final long[][] nodeCapacities = new long[numResources][];
        final long[] maxCapacities = new long[numResources];

        for (int i = 0; i < capacities.size(); i++) {
            final List<Long> vec = capacities.get(i);
            final long[] capacityArr = new long[vec.size()];
            long maxCapacityValue = Long.MIN_VALUE;
            for (int j = 0; j < capacityArr.length; j++) {
                capacityArr[j] = vec.get(j);
                maxCapacityValue = Math.max(maxCapacityValue, capacityArr[j]);
            }
            nodeCapacities[i] = capacityArr;
            maxCapacities[i] = maxCapacityValue;
        }

        // For each resource, create dummy demands to accommodate heterogeneous capacities
        final long[][] updatedDemands = new long[numResources][];
        for (int i = 0; i < numResources; i++) {
            final long[] demand = new long[numTasks + capacities.get(0).size()];

            // copy over task demands
            int iter = 0;
            for (final long taskDemand: demands.get(i)) {
                demand[iter] = taskDemand;
                iter++;
            }

            // copy over dummy demands
            final long maxCapacity = maxCapacities[i];
            for (final long nodeHeterogeneityAdjustment: nodeCapacities[i]) {
                demand[iter] = maxCapacity - nodeHeterogeneityAdjustment;
                iter++;
            }
            updatedDemands[i] = demand;
        }

        // 2. Capacity constraints
        for (int i = 0; i < numResources; i++) {
            model.addCumulative(tasksIntervals, updatedDemands[i], maxCapacities[i]);
        }

        // Cumulative score
        for (int i = 0; i < numResources; i++) {
            final IntVar max = model.newIntVar(0, maxCapacities[i], "");
            model.addCumulative(tasksIntervals, updatedDemands[i], max);
            model.minimize(max);
        }
    }

    public void maximize(final IntVar var) {
        model.maximize(var);
    }

    public void maximize(final List<IntVar> list) {
        list.forEach(model::maximize);
    }

    public IntVar newIntVar(final String name) {
        return model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, name);
    }

    public IntVar newIntVar(final long lb, final long ub, final String name) {
        return model.newIntVar(lb, ub, name);
    }

    /*
     * Assumes var is true
     */
    public void assume(final IntVar var, final String assumptionContext) {
        var.getBuilder().setName(assumptionContext);
        model.addAssumption(var);
    }

    /*
     * Assume "left implies right" is true
     */
    public void assumeImplication(final IntVar left, final IntVar right, final String assumptionContext) {
        final Literal assumptionLiteral = model.newBoolVar(assumptionContext);
        model.addImplication(left, right).onlyEnforceIf(assumptionLiteral);
        model.addAssumption(assumptionLiteral);
    }

    /*
     * Returns an array of IntVars that assumes it can mirror the values of the input IntVars.
     *
     * We use this to add assumption literals for constraints that do not support enforcement literals.
     */
    private IntVar[] assumptionLinkedVars(final IntVar[] input, final String assumptionContext) {
        final IntVar[] output = new IntVar[input.length];
        final IntVar[] assumptionLiterals = new IntVar[input.length];
        for (int i = 0; i < input.length; i++) {
            output[i] = model.newIntVarFromDomain(input[i].getDomain(), "");
            assumptionLiterals[i] = model.newBoolVar(assumptionContext);
            model.addEquality(output[i], input[i]).onlyEnforceIf(assumptionLiterals[i]);
        }
        model.addAssumptions(assumptionLiterals);
        return output;
    }

    /*
     * Returns an array of IntVars that assumes, it can mirror the values of the input IntVars.
     *
     * We use this to add assumption literals for constraints that do not support enforcement literals.
     */
    public List<String> findSufficientAssumptions(final CpSolver solver) {
        // For the assumptions interface to work, there should be no objective functions,
        // and there should be only a single search worker.
        // Please see: https://github.com/google/or-tools/issues/2563
        solver.getParameters().setNumSearchWorkers(1);
        model.getBuilder().getObjective().toBuilder().clear();

        // Resolve with updated model
        final CpSolverStatus solve = solver.solve(model);
        Preconditions.checkArgument(solve == CpSolverStatus.INFEASIBLE,
                "Ops.sufficientAssumptions() should not be invoked unless the model is UNSAT");
        return solver.sufficientAssumptionsForInfeasibility().stream()
                 .map(model.getBuilder()::getVariables)
                 .map(IntegerVariableProto::getName)
                 .collect(Collectors.toList());
    }
}