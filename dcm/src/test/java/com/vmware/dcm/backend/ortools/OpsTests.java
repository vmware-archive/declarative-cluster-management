/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("ConstantConditions")
public class OpsTests {

    static {
        // Causes or-tools JNI library to be loaded
        final OrToolsSolver builder = new OrToolsSolver.Builder().build();
        System.out.println(builder);
    }

    @Nullable private CpModel model;
    @Nullable private StringEncoding encoding;
    @Nullable private Ops ops;

    @BeforeEach
    public void setupOps() {
        model = new CpModel();
        encoding = new StringEncoding();
        ops = new Ops(model, encoding);
    }

    @Test
    public void testSum() {
        final List<Integer> entries = List.of(1, 2, 3, 4);
        assertEquals(10, ops.sumInteger(entries));
    }

    @Test
    public void testScalProdInt() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""), model.newIntVar(1, 2, ""));
        final List<Integer> coeff = List.of(5, 6);
        final IntVar scalProd = ops.scalProdInteger(entries, coeff);
        model.maximize(scalProd);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(17, solver.value(scalProd));
    }

    @Test
    public void testScalProdLong() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""), model.newIntVar(1, 2, ""));
        final List<Long> coeff = List.of(5L, 6L);
        final IntVar scalProd = ops.scalProdLong(entries, coeff);
        model.maximize(scalProd);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(17L, solver.value(scalProd));
    }

    @Test
    public void testSumVEquals() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""),
                                             model.newIntVar(1, 2, ""),
                                             model.newIntVar(2, 3, ""),
                                             model.newIntVar(3, 4, ""));
        final IntVar sum = ops.sumV(entries);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);

        final long total = entries.stream().map(solver::value).reduce(Long::sum).get();
        assertEquals(total, solver.value(sum));
    }

    @Test
    public void testSumVConstraintOnReturn() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""),
                                             model.newIntVar(1, 2, ""),
                                             model.newIntVar(2, 3, ""),
                                             model.newIntVar(3, 4, ""));
        final IntVar sum = ops.sumV(entries);
        model.addGreaterOrEqual(sum, 8L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);

        final long total = entries.stream().map(solver::value).reduce(Long::sum).get();
        assertEquals(8, total);
        assertEquals(8, solver.value(sum));
    }

    @Test
    public void testIncreasing() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 100, ""),
                                             model.newIntVar(0, 100, ""),
                                             model.newIntVar(0, 100, ""),
                                             model.newIntVar(0, 100, ""));
        ops.increasing(entries);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);

        for (int i = 0; i < entries.size() - 1; i++) {
            assertTrue(solver.value(entries.get(i)) <= solver.value(entries.get(i + 1)));
        }
    }

    @ParameterizedTest
    @MethodSource("existsMethodSource")
    public void testExists(final boolean exists) {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""));
        final IntVar existsVar = ops.exists(entries);
        model.addEquality(existsVar, model.newConstant(exists ? 1L : 0L));
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(exists ? 1L : 0L, solver.value(existsVar));

        final Set<Long> assignments = entries.stream().map(solver::value).collect(Collectors.toSet());
        assertEquals(exists, assignments.contains(1L));
    }

    static Stream existsMethodSource() {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    @Test
    public void maxV() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 10, ""),
                                             model.newIntVar(0, 10, ""),
                                             model.newIntVar(0, 10, ""),
                                             model.newIntVar(0, 10, ""));
        final IntVar maxV = ops.maxVIntVar(entries);
        model.addEquality(maxV, 9);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        final long largestValue = entries.stream().map(solver::value).max(Long::compareTo).get();
        assertEquals(largestValue, solver.value(maxV));
        assertEquals(9, largestValue);
    }

    @Test
    public void maxVInequality() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""));
        final IntVar maxV = ops.maxVIntVar(entries);
        model.addDifferent(maxV, 1);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        final long largestValue = entries.stream().map(solver::value).max(Long::compareTo).get();
        assertEquals(largestValue, solver.value(maxV));
        assertEquals(0, largestValue);
    }


    @Test
    public void maxVPrimitives() {
        final List<Integer> entries = List.of(1, 2, 4, 3);
        assertEquals(4, ops.maxVInteger(entries));

        final List<Long> entriesLong = List.of(1L, 2L, 4L, 3L);
        assertEquals(4L, ops.maxVLong(entriesLong));
    }

    @Test
    public void minV() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 10, ""),
                                             model.newIntVar(0, 10, ""),
                                             model.newIntVar(0, 10, ""),
                                             model.newIntVar(0, 10, ""));
        final IntVar minV = ops.minVIntVar(entries);
        model.addEquality(minV, 9);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        final long largestValue = entries.stream().map(solver::value).min(Long::compareTo).get();
        assertEquals(largestValue, solver.value(minV));
        assertEquals(9, largestValue);
    }

    @Test
    public void minVInequality() {
        final List<IntVar> entries = List.of(model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""),
                                             model.newIntVar(0, 1, ""));
        final IntVar minV = ops.minVIntVar(entries);
        model.addDifferent(minV, 0);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        final long largestValue = entries.stream().map(solver::value).min(Long::compareTo).get();
        assertEquals(largestValue, solver.value(minV));
        assertEquals(1, largestValue);
    }

    @Test
    public void minVPrimitives() {
        final List<Integer> entries = List.of(1, 2, 4, 3);
        assertEquals(1, ops.minVInteger(entries));

        final List<Long> entriesLong = List.of(1L, 2L, 4L, 3L);
        assertEquals(1L, ops.minVLong(entriesLong));
    }

    @Test
    public void countV() {
        final long[] entries = {1L, 2L};
        assertEquals(2, ops.countV(entries));
    }

    @Test
    public void minusVars() {
        final IntVar left = model.newIntVar(0, 1, "");
        final IntVar right = model.newIntVar(100, 100, "");
        final IntVar minus = ops.minus(left, right);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(minus), solver.value(left) - solver.value(right));
    }

    @Test
    public void minusVarsWithEquality() {
        final IntVar left = model.newIntVar(0, 1, "");
        final IntVar right = model.newIntVar(100, 100, "");
        final IntVar minus = ops.minus(left, right);
        model.addEquality(minus, -100);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(minus), solver.value(left) - solver.value(right));
        assertEquals(-100L, solver.value(minus));
        assertEquals(0L, solver.value(left));
        assertEquals(100L, solver.value(right));
    }

    @Test
    public void minusVarsWithInequality() {
        final IntVar left = model.newIntVar(0, 1, "");
        final IntVar right = model.newIntVar(100, 100, "");
        final IntVar minus = ops.minus(left, right);
        model.addDifferent(minus, -100);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(minus), solver.value(left) - solver.value(right));
        assertEquals(-99L, solver.value(minus));
        assertEquals(1L, solver.value(left));
        assertEquals(100L, solver.value(right));
    }

    @Test
    public void minusVarWithConstantLeft() {
        final int left = 1;
        final IntVar right = model.newIntVar(99, 100, "");
        final IntVar minus = ops.minus(left, right);
        model.addDifferent(minus, -98);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(minus), left - solver.value(right));
        assertEquals(-99L, solver.value(minus));
        assertEquals(100L, solver.value(right));
    }

    @Test
    public void minusVarWithConstantRight() {
        final IntVar left = model.newIntVar(99, 100, "");
        final int right = 1;
        final IntVar minus = ops.minus(left, right);
        model.addDifferent(minus, 98);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(minus), solver.value(left) - right);
        assertEquals(99L, solver.value(minus));
        assertEquals(100L, solver.value(left));
    }


    @Test
    public void multVars() {
        final IntVar left = model.newIntVar(-100, 100, "");
        final IntVar right = model.newIntVar(100, 100, "");
        final IntVar mult = ops.mult(left, right);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(mult), solver.value(left) * solver.value(right));
    }


    @Test
    public void multVarsWithEquality() {
        final IntVar left = model.newIntVar(0, 10, "");
        final IntVar right = model.newIntVar(0, 10, "");
        final IntVar mult = ops.mult(left, right);

        model.addEquality(mult, 21L);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(mult), solver.value(left) * solver.value(right));
        assertTrue(solver.value(left) == 3L || solver.value(right) == 3L);
        assertTrue(solver.value(left) == 7L || solver.value(right) == 7L);
    }

    @Test
    public void multVarsWithDifferent() {
        final IntVar left = model.newIntVar(0, 1, "");
        final IntVar right = model.newIntVar(0, 1, "");
        final IntVar mult = ops.mult(left, right);

        model.addDifferent(mult, 0L);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(mult), solver.value(left) * solver.value(right));
        assertTrue(solver.value(left) == 1L && solver.value(right) == 1L);
    }

    @Test
    public void multVarsWithConstantLeft() {
        final int left = 10;
        final IntVar right = model.newIntVar(50, 100, "");
        final IntVar mult = ops.mult(left, right);

        model.addEquality(mult, 700L);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(mult), left * solver.value(right));
        assertEquals(70L, solver.value(right));
    }

    @Test
    public void multVarsWithConstantRight() {
        final IntVar left = model.newIntVar(50, 100, "");
        final int right = 10;
        final IntVar mult = ops.mult(left, right);

        model.addEquality(mult, 700L);
        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(solver.value(mult), solver.value(left) * right);
        assertEquals(70L, solver.value(left));
    }

    @Test
    public void eqPrimitives() {
        assertTrue(ops.eq(1, 1));
        assertTrue(ops.eq(1L, 1L));
        assertTrue(ops.eq(true, true));
        assertTrue(ops.eq("1", "1"));
        assertFalse(ops.eq(1, 2));
        assertFalse(ops.eq(1L, 2L));
        assertFalse(ops.eq(true, false));
        assertFalse(ops.eq("1", "2"));
        assertFalse(ops.ne(1, 1));
        assertFalse(ops.ne(1L, 1L));
        assertFalse(ops.ne(true, true));
        assertFalse(ops.ne("1", "1"));
        assertTrue(ops.ne(1, 2));
        assertTrue(ops.ne(1L, 2L));
        assertTrue(ops.ne(true, false));
        assertTrue(ops.ne("1", "2"));
    }

    @Test
    public void eqVarStrWithEquality() {
        final long l = encoding.toLong("hello");
        final IntVar right = model.newIntVar(0, l * 2, "");

        final IntVar eq = ops.eq("hello", right);
        model.addEquality(eq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals("hello", encoding.toStr(solver.value(right)));
    }

    @Test
    public void eqVarStrWithDifferent() {
        final long l = encoding.toLong("hello");
        final IntVar right = model.newIntVar(l, l + 1, "");

        final IntVar eq = ops.eq("hello", right);
        model.addDifferent(eq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(l + 1, solver.value(right));
    }

    @Test
    public void eqVarLongWithEquality() {
        final IntVar right = model.newIntVar(0, 20, "");

        final IntVar eq = ops.eq(10L, right);
        model.addEquality(eq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(10L, solver.value(right));
    }

    @Test
    public void eqVarBooleanTrueWithEquality() {
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar eq = ops.eq(true, right);
        model.addEquality(eq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(1L, solver.value(right));
    }

    @Test
    public void eqVarBooleanFalseWithEquality() {
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar eq = ops.eq(false, right);
        model.addEquality(eq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(0L, solver.value(right));
    }

    @Test
    public void eqVarBooleanTrueNegated() {
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar eq = ops.eq(true, right);
        model.addEquality(eq, 0L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(0L, solver.value(right));
    }

    @Test
    public void neVarStrWithEquality() {
        final long l = encoding.toLong("hello");
        final IntVar right = model.newIntVar(0, l * 2, "");

        final IntVar ne = ops.ne("hello", right);
        model.addEquality(ne, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertNull(encoding.toStr(solver.value(right)));
    }

    @Test
    public void neVarStrWithDifferent() {
        final long l = encoding.toLong("hello");
        final IntVar right = model.newIntVar(l, l + 1, "");

        final IntVar ne = ops.ne("hello", right);
        model.addDifferent(ne, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals("hello", encoding.toStr(solver.value(right)));
    }


    @Test
    public void neVarLongWithEquality() {
        final IntVar right = model.newIntVar(10, 11, "");

        final IntVar ne = ops.ne(10L, right);
        model.addEquality(ne, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(11L, solver.value(right));
    }

    @Test
    public void neVarBooleanTrueWithEquality() {
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar ne = ops.ne(true, right);
        model.addEquality(ne, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(0L, solver.value(right));
    }

    @Test
    public void neVarBooleanFalseWithEquality() {
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar ne = ops.ne(false, right);
        model.addEquality(ne, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(1L, solver.value(right));
    }

    @Test
    public void neVarBooleanTrueNegated() {
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar ne = ops.ne(true, right);
        model.addEquality(ne, 0L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(1L, solver.value(right));
    }

    @Test
    public void ltWithConstantRight() {
        final IntVar left = model.newIntVar(0, 1L, "");
        final IntVar lt = ops.lt(left, 1);
        model.addEquality(lt, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(0L, solver.value(left));
    }

    @Test
    public void ltWithVar() {
        final IntVar left = model.newIntVar(0, 1L, "");
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar lt = ops.lt(left, right);
        model.addEquality(lt, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(0L, solver.value(left));
        assertEquals(1L, solver.value(right));
    }

    @Test
    public void leqWithConstantRight() {
        final IntVar left = model.newIntVar(0, 100L, "");
        final IntVar leq = ops.leq(left, 1);
        model.addEquality(leq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertTrue(solver.value(left) == 0 || solver.value(left) == 1L);
    }

    @Test
    public void leqWithVar() {
        final IntVar left = model.newIntVar(0, 1L, "");
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar leq = ops.leq(left, right);
        model.addEquality(leq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertFalse(solver.value(left) == 1L && solver.value(right) == 1L);
    }

    @Test
    public void gtWithConstantRight() {
        final IntVar left = model.newIntVar(0, 2L, "");
        final IntVar gt = ops.gt(left, 1);
        model.addEquality(gt, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(2L, solver.value(left));
    }

    @Test
    public void gtWithVar() {
        final IntVar left = model.newIntVar(0, 1L, "");
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar gt = ops.gt(left, right);
        model.addEquality(gt, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(1L, solver.value(left));
        assertEquals(0L, solver.value(right));
    }

    @Test
    public void geqWithConstantRight() {
        final IntVar left = model.newIntVar(0, 100L, "");
        final IntVar geq = ops.geq(left, 99L);
        model.addEquality(geq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertTrue(solver.value(left) == 99L || solver.value(left) == 100L);
    }

    @Test
    public void geqWithVar() {
        final IntVar left = model.newIntVar(0, 1L, "");
        final IntVar right = model.newIntVar(0, 1L, "");

        final IntVar geq = ops.geq(left, right);
        model.addEquality(geq, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertFalse(solver.value(left) == 0L && solver.value(right) == 1L);
    }

    @Test
    public void inString() {
        final List<String> strings = List.of("1", "2", "3", "4", "5", "6");
        strings.forEach(encoding::toLong);
        final IntVar var = model.newIntVar(0, 6L, "");

        final IntVar in = ops.inString(var, List.of("1", "5"));
        model.addEquality(in, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertTrue(encoding.toStr(solver.value(var)).equals("1")
                || encoding.toStr(solver.value(var)).equals("5"));
    }

    @Test
    public void inLong() {
        final List<Long> longs = List.of(2L, 3L);
        final IntVar var = model.newIntVar(0, 6L, "");

        final IntVar in = ops.inLong(var, longs);
        model.addEquality(in, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertTrue(longs.contains(solver.value(var)));
    }

    @Test
    public void inInt() {
        final List<Integer> ints = List.of(2, 3);
        final IntVar var = model.newIntVar(0, 6, "");

        final IntVar in = ops.inInteger(var, ints);
        model.addEquality(in, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertTrue(ints.contains((int) solver.value(var)));
    }

    @Test
    public void inIntNegated() {
        final List<Integer> ints = List.of(2, 3);
        final IntVar var = model.newIntVar(0, 6, "");

        final IntVar in = ops.inInteger(var, ints);
        model.addEquality(in, 0L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertFalse(ints.contains((int) solver.value(var)));
    }

    @Test
    public void inIntVar() {
        final List<IntVar> vars = List.of(model.newIntVar(0, 2, ""),
                                          model.newIntVar(3, 5, ""),
                                          model.newIntVar(7, 8, ""));
        final IntVar var = model.newIntVar(-100, 100, "");

        final IntVar in = ops.inIntVar(var, vars);
        model.addEquality(in, 1L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        final Set<Long> collect = vars.stream().map(solver::value).collect(Collectors.toSet());
        assertTrue(collect.contains(solver.value(var)));
    }

    @Test
    public void inIntVarNegated() {
        final List<IntVar> vars = List.of(model.newIntVar(0, 0, ""),
                                          model.newIntVar(4, 6, ""),
                                          model.newIntVar(7, 8, ""));
        final IntVar var = model.newIntVar(0, 10, "");

        final IntVar in = ops.inIntVar(var, vars);
        model.addEquality(in, 0L);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        final Set<Long> collect = vars.stream().map(solver::value).collect(Collectors.toSet());
        assertFalse(collect.contains(solver.value(var)));
    }

    @Test
    public void inObjectArrHardConstraint() {
        final Object[] vars = {100};
        final IntVar var = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        final IntVar in = ops.inObjectArr(var, vars);
        model.addEquality(in, 1);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(100, solver.value(var));
    }

    @Test
    public void inObjectArrSoft() {
        final Object[] vars = {100};
        final IntVar var = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
        final IntVar in = ops.inObjectArr(var, vars);
        model.maximize(in);

        final CpSolver solver = new CpSolver();
        final CpSolverStatus solve = solver.solve(model);
        assertEquals(CpSolverStatus.OPTIMAL, solve);
        assertEquals(100, solver.value(var));
    }
}
