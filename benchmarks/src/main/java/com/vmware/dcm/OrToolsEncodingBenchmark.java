/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.jooq.impl.DSL.using;

@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)

public class OrToolsEncodingBenchmark {
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Nullable Model model = null;

        @Param({"true", "false"})
        static boolean useCumulative;

        @Param({"true", "false"})
        static boolean useScalarProduct;

        @Setup(Level.Iteration)
        public void setUp() {
            // create database
            final DSLContext conn = setup();

            conn.execute("CREATE TABLE t1 (" +
                    "c1 integer PRIMARY KEY, " +
                    "c2 integer " +
                    ")"
            );
            conn.execute("CREATE TABLE t2 (" +
                    "controllable__c1 integer null, " +
                    "d1 integer, " +
                    "FOREIGN KEY(controllable__c1) REFERENCES t1(c1)" +
                    ")"
            );

            final List<String> views = useCumulative ? List.of(
                    "create constraint constraint_c1 as " +
                            "select * from t2 " +
                            "join t1 " +
                            "     on t2.controllable__c1 = t1.c1 " +
                            "check capacity_constraint(t2.controllable__c1, t1.c1, " +
                            "                           t2.d1, t1.c2) = true",
                    "create constraint constraint_symmetry as " +
                            "select * from t2 check increasing(controllable__c1) = true"
            ) : List.of(
                    "create constraint load_view as " +
                            "select t1.c2 as capacity, sum(t2.d1) as load from t2 " +
                            "join t1 " +
                            "     on t2.controllable__c1 = t1.c1 " +
                            "group by t1.c1, t1.c2",
                    "create constraint constraint_c1 as " +
                            "select * from load_view " +
                            "check load <= capacity",
                    "create constraint objective_c2 as " +
                            "select min(load) from load_view maximize",
                    "create constraint constraint_symmetry as " +
                            "select * from t2 check increasing(controllable__c1) = true"
            );

            for (int i = 0; i < 5000; i++) {
                conn.execute(String.format("insert into t1 values(%s, %s)", i,
                        50 + ThreadLocalRandom.current().nextInt(1, 100)));
            }
            for (int i = 0; i < 50; i++) {
                conn.execute(
                        String.format("insert into t2 values(null, %s)",
                                ThreadLocalRandom.current().nextInt(1, 50)));
            }

            final OrToolsSolver solver = new OrToolsSolver.Builder()
                    .setTryScalarProductEncoding(useScalarProduct)
                    .setMaxTimeInSeconds(100)
                    .build();
            model = Model.build(conn, solver, views);
        }

        @CanIgnoreReturnValue
        private Connection getConnection(final String url, final Properties properties) throws SQLException {
            return DriverManager.getConnection(url, properties);
        }

        private DSLContext setup() {
            final Properties properties = new Properties();
            properties.setProperty("foreign_keys", "true");
            try {
                // Create a fresh database
                final String connectionURL = "jdbc:h2:mem:;create=true";
                final Connection conn = getConnection(connectionURL, properties);
                final DSLContext using = using(conn, SQLDialect.H2);
                using.execute("create schema curr");
                using.execute("set schema curr");
                return using;
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Benchmark
    public void runSolver(final OrToolsEncodingBenchmark.BenchmarkState state) {
        assert state.model != null;
        state.model.solve("T2");
    }
}