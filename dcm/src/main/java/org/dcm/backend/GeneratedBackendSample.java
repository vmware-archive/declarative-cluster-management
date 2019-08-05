/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;


import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import com.google.ortools.util.Domain;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.Model;
import org.dcm.ModelException;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Record4;
import org.jooq.Result;

import javax.annotation.processing.Generated;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Generated("org.dcm.backend.OrToolsSolver")
public final class GeneratedBackendSample implements IGeneratedBackend {
    public Map<IRTable, Result<? extends Record>> solve(final IRContext context) {
        // Create the model.
        final long startTime = System.nanoTime();
        final CpModel model = new CpModel();
        final StringEncoding encoder = new StringEncoding();
        final Ops o = new Ops(model, encoder);

        // Foreign key constraints: POD_INFO.CONTROLLABLE__NODE_NAME -> NODE_INFO.NAME
        final long[] domain = context.getTable("NODE_INFO").getCurrentData()
                .getValues("NAME", String.class)
                .stream()
                .mapToLong(encoder::toLong)
                .distinct()
                .toArray();
        final Domain domain1 = Domain.fromValues(domain);

        // Table "POD_INFO"
        final List<Record4<String, String, String, Integer>> podInfo = (List<Record4<String, String, String, Integer>>) context.getTable("POD_INFO").getCurrentData();
        final IntVar[] podInfoControllableNodeName = new IntVar[podInfo.size()];
        for (int i = 0; i < podInfo.size(); i++) {
            podInfoControllableNodeName[i] = model.newIntVarFromDomain(domain1, "CONTROLLABLE__NODE_NAME");
        }

        // Table "NODE_INFO"
        final List<Record2<String, Integer>> nodeInfo = (List<Record2<String, Integer>>) context.getTable("NODE_INFO").getCurrentData();

        System.out.println("Array declarations: we are at " + (System.nanoTime() - startTime));

        // Non-constraint view tmp1
        final Map<Tuple2<String, Integer>, List<Tuple4<Integer, Integer, IntVar, String>>> tmp1 = new HashMap<>();
        for (int podInfoIter = 0; podInfoIter < podInfo.size(); podInfoIter++) {
            for (int nodeInfoIter = 0; nodeInfoIter < nodeInfo.size(); nodeInfoIter++) {
                if ((podInfo.get(podInfoIter).get("STATUS", String.class).equals("Pending"))) {
                    final Tuple4<Integer, Integer, IntVar, String> tuple = new Tuple4<>(
                            nodeInfo.get(nodeInfoIter).get("CPU_ALLOCATABLE", Integer.class) /* CPU_ALLOCATABLE */,
                            podInfo.get(podInfoIter).get("CPU_REQUEST", Integer.class) /* CPU_REQUEST */,
                            podInfoControllableNodeName[podInfoIter] /* CONTROLLABLE__NODE_NAME */,
                            nodeInfo.get(nodeInfoIter).get("NAME", String.class) /* NAME */
                    );
                    final Tuple2<String, Integer> groupByTuple = new Tuple2<>(
                            nodeInfo.get(nodeInfoIter).get("NAME", String.class),
                            nodeInfo.get(nodeInfoIter).get("CPU_ALLOCATABLE", Integer.class)
                    );
                    tmp1.computeIfAbsent(groupByTuple, (k) -> new ArrayList<>()).add(tuple);
                }
            }
        }

        System.out.println("Group-by intermediate view: we are at " + (System.nanoTime() - startTime));
        // Non-constraint view podsDemandPerNode
        final List<Tuple1<IntVar>> podsDemandPerNode = new ArrayList<>(tmp1.size());
        for (final Map.Entry<Tuple2<String, Integer>, List<Tuple4<Integer, Integer, IntVar, String>>> entry: tmp1.entrySet()) {
            final Tuple2<String, Integer> group = entry.getKey();
            final List<Tuple4<Integer, Integer, IntVar, String>> data = entry.getValue();
            final IntVar[] bools = data.stream().map(e -> o.eq(e.value2(), e.value3())).toArray(IntVar[]::new);
            final long[] longs = data.stream().mapToLong(Tuple4::value1).toArray();
            final IntVar result = model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, "");
            final LinearExpr linearExpr = LinearExpr.scalProd(bools, longs);
            model.addEquality(result, linearExpr);
//            final Tuple1<IntVar> res = new Tuple1<>(o.minus(group.value1(), result));
//            podsDemandPerNode.add(res);
            model.addGreaterThan(result, group.value1());
        }

        final IntVar max = model.newIntVar(0, 1000000000, "");
        model.addMaxEquality(max, podsDemandPerNode.stream().map(Tuple1::value0).toArray(IntVar[]::new));
        model.minimize(max);

        System.out.println("Group-by final view: we are at " + (System.nanoTime() - startTime));

        // Start solving
        System.out.println("Model creation: we are at " + (System.nanoTime() - startTime));
        System.out.println("CON COUNT " + model.model().getConstraintsCount());
        System.out.println("VAR COUNT " + model.model().getVariablesCount());
        final CpSolver solver = new CpSolver();
        solver.getParameters().setLogSearchProgress(true);
        final CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL) {
            final Map<IRTable, Result<? extends Record>> result = new HashMap<>();
            final Object[] obj = new Object[1]; // Used to update controllable fields;
            final Result<? extends Record> tmp3 = context.getTable("POD_INFO").getCurrentData();
            for (int i = 0; i < podInfo.size(); i++) {
                obj[0] = encoder.toStr(solver.value(podInfoControllableNodeName[i]));
                tmp3.get(i).from(obj, "CONTROLLABLE__NODE_NAME");
            }
            result.put(context.getTable("POD_INFO"), tmp3);
            result.put(context.getTable("NODE_INFO"), context.getTable("NODE_INFO").getCurrentData());
            result.put(context.getTable("GROUP_TABLE__PODS_DEMAND_PER_NODE"),
                       context.getTable("GROUP_TABLE__PODS_DEMAND_PER_NODE").getCurrentData());
            return result;
        }
        throw new ModelException("Could not solve " + status);
    }


    private final class Tuple1<T0> {
        private final T0 t0;

        private Tuple1(final T0 t0) {
            this.t0 = t0;
        }

        T0 value0() {
            return t0;
        }

        @Override
        public String toString() {
            return String.format("(%s)", t0);
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof Tuple1)) {
                return false;
            }
            final Tuple1 that = (Tuple1) other;
            return this.value0().equals(that.value0());
        }
    }

    private final class Tuple2<T0, T1> {
        private final T0 t0;

        private final T1 t1;

        private Tuple2(final T0 t0, final T1 t1) {
            this.t0 = t0;
            this.t1 = t1;
        }

        T0 value0() {
            return t0;
        }

        T1 value1() {
            return t1;
        }

        @Override
        public String toString() {
            return String.format("(%s)", t0, t1);
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof Tuple2)) {
                return false;
            }
            final Tuple2 that = (Tuple2) other;
            return this.value0().equals(that.value0()) && this.value1().equals(that.value1());
        }
    }

    private final class Tuple4<T0, T1, T2, T3> {
        private final T0 t0;

        private final T1 t1;

        private final T2 t2;

        private final T3 t3;

        private Tuple4(final T0 t0, final T1 t1, final T2 t2, final T3 t3) {
            this.t0 = t0;
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
        }

        T0 value0() {
            return t0;
        }

        T1 value1() {
            return t1;
        }

        T2 value2() {
            return t2;
        }

        T3 value3() {
            return t3;
        }

        @Override
        public String toString() {
            return String.format("(%s)", t0, t1, t2, t3);
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof Tuple4)) {
                return false;
            }
            final Tuple4 that = (Tuple4) other;
            return this.value0().equals(that.value0()) && this.value1().equals(that.value1()) && this.value2().equals(that.value2()) && this.value3().equals(that.value3());
        }
    }
}

