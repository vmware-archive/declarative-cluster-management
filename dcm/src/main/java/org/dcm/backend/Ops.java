/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import com.google.ortools.sat.Literal;
import com.google.ortools.util.Domain;

import java.util.Arrays;
import java.util.List;

public class Ops {
    private final CpModel model;
    private final StringEncoding encoder;

    public Ops(final CpModel model, final StringEncoding encoding) {
        this.model = model;
        this.encoder = encoding;
    }

    public int sum(final List<Integer> data) {
        int ret = 0;
        for (final Integer d: data) {
            ret += d;
        }
        return ret;
    }

    public IntVar sumV(final List<IntVar> data) {
        final IntVar ret = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        model.addEquality(ret, LinearExpr.sum(data.toArray(new IntVar[0])));
        return ret;
    }

    public void increasing(final List<IntVar> data) {
        for (int i = 0; i < data.size() - 1; i++) {
            model.addLessOrEqual(data.get(i), data.get(i + 1));
        }
    }

    public IntVar exists(final List<IntVar> data) {
        final IntVar bool = model.newBoolVar("");
        final Literal[] literals = data.toArray(new Literal[0]);
        model.addBoolOr(literals).onlyEnforceIf(bool);
        model.addBoolAnd(data.stream().map(IntVar::not).toArray(Literal[]::new)).onlyEnforceIf(bool.not());
        return bool;
    }


    public int maxV(final long[] data) {
        return (int) Arrays.stream(data).max().getAsLong();
    }

    public int countV(final long[] data) {
        return data.length;
    }

    public IntVar minus(final int left, final IntVar right) {
        return minus(right, left);
    }

    public IntVar minus(final IntVar left, final int right) {
        final IntVar ret = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        model.addEquality(ret, LinearExpr.sum(new IntVar[]{left, model.newConstant(-right)}));
        return ret;
    }

    public IntVar minus(final IntVar left, final IntVar right) {
        final IntVar ret = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        model.addEquality(ret, LinearExpr.scalProd(new IntVar[]{left, right}, new int[]{1, -1}));
        return ret;
    }


    public IntVar mult(final int left, final IntVar right) {
        return mult(right, left);
    }

    public IntVar mult(final IntVar left, final int right) {
        final IntVar ret = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        model.addEquality(ret, LinearExpr.scalProd(new IntVar[]{left}, new int[]{right}));
        return ret;
    }

    public IntVar mult(final IntVar left, final IntVar right) {
        final IntVar ret = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        model.addProductEquality(ret, new IntVar[]{left, right});
        return ret;
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
        return eq(left, model.newConstant(right ? 1 : 0));
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

    public IntVar ne(final IntVar left, final boolean right) {
        return ne(left, model.newConstant(right ? 1 : 0));
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

    public IntVar in(final IntVar left, final List<String> right, final String instance) {
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain);
        return bool;
    }

    public IntVar in(final IntVar left, final List<Long> right, final long instance) {
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain);
        return bool;
    }

    public IntVar in(final IntVar left, final List<Integer> right, final int instance) {
        final IntVar bool = model.newBoolVar("");
        final Domain domain = Domain.fromValues(right.stream().mapToLong(encoder::toLong).toArray());
        model.addLinearExpressionInDomain(left, domain);
        return bool;
    }

    public IntVar or(final boolean left, final IntVar right) {
        return or(model.newConstant(left ? 1 : 0), right);
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
        return and(model.newConstant(left ? 1 : 0), right);
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

    public IntVar toConst(final boolean expr) {
        return expr ? model.newConstant(1) : model.newConstant(0);
    }

    public IntVar toConst(final long expr) {
        return model.newConstant(expr);
    }
}