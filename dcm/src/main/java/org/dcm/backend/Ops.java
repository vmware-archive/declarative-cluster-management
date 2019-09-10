/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;

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

    public IntVar eq(final String left, final IntVar right) {
        return eq(right, left);
    }

    public IntVar eq(final IntVar left, final String right) {
        return eq(left, encoder.toLong(right));
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
}