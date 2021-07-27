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

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)

public class OrToolsIndexBenchmark {
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Nullable Model model = null;

        @Param({"true", "false"})
        static boolean useIndex;

        @Setup(Level.Iteration)
        public void setUp() {
            // create database
            final DSLContext conn = setup();

            conn.execute("create table t1(c1 integer, c2 integer, primary key (c1))");
            conn.execute("create table t2(c1 integer, controllable__c2 integer, primary key (c1))");

            final List<String> views = List.of(
                    "CREATE CONSTRAINT constraint_with_join AS " +
                    "SELECT * FROM t1 " +
                    "JOIN t2 on t1.c1 = t2.c1 " +
                    "check c2 = controllable__c2"
            );


            for (int i = 0; i < 1000; i++) {
                conn.execute(String.format("insert into t1 values(%s, %s)", i,
                        50 + ThreadLocalRandom.current().nextInt(1, 100)));
            }
            for (int i = 0; i < 5000; i++) {
                conn.execute(String.format("insert into t2 values(%s, null)", i));
            }

            final OrToolsSolver solver = new OrToolsSolver.Builder()
                    .setUseIndicesForEqualityBasedJoins(useIndex)
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
    public void runSolver(final BenchmarkState state) {
        assert state.model != null;
        state.model.solve("T2");
    }
}