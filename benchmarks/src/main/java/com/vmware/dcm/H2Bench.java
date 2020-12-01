/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.sql.DriverManager.getConnection;
import static org.jooq.impl.DSL.using;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class H2Bench {
    private static final int NUM_TABLES = 30;

    final DBConnectionPool dbConnectionPool = setupDbConnectionPool();
    final DSLContext conn = setup();
    final AtomicInteger integer = new AtomicInteger(0);

    @Setup(Level.Invocation)
    public void setUpDb() {
        for (int t = 0; t < NUM_TABLES; t++) {
            conn.execute(String.format("delete from t%s where c2 >= 0", t));
            for (int i = 0; i < 1000; i++) {
                final int val = integer.incrementAndGet();
                conn.execute(String.format("insert into t%s values ('%s', %s)", t, val, val));
            }
        }
    }

    private DBConnectionPool setupDbConnectionPool() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        for (int t = 0; t < NUM_TABLES; t++) {
            dbConnectionPool.getConnectionToDb()
                    .execute(String.format("create table t%s (c1 varchar(36) primary key, c2 integer)", t));
        }
        return dbConnectionPool;
    }

    private DSLContext setup() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:create=true";
            final Connection conn = getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.H2);
            using.execute("create schema curr");
            using.execute("set schema curr");
            for (int t = 0; t < NUM_TABLES; t++) {
                using.execute(String.format("create table t%s (c1 varchar(36) primary key, c2 integer)", t));
            }
            return using;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void simple(final Blackhole blackhole) {
        IntStream.range(0, NUM_TABLES).forEach(
            t -> {
                final Result<Record> fetch = conn.selectFrom("t" + t).fetch();
                blackhole.consume(fetch);
            }
        );
    }

    @Benchmark
    public void withPool(final Blackhole blackhole) {
        IntStream.range(0, NUM_TABLES).forEach(
            t -> {
                final Result<Record> fetch = dbConnectionPool.getConnectionToDb().selectFrom("t" + t).fetch();
                blackhole.consume(fetch);
            }
        );
    }

    @Benchmark
    public void simpleParallel(final Blackhole blackhole) {
        IntStream.range(0, NUM_TABLES).parallel().forEach(
            t -> {
                final Result<Record> fetch = conn.selectFrom("t" + t).fetch();
                blackhole.consume(fetch);
            }
        );
    }

    @Benchmark
    public void withPoolParallel(final Blackhole blackhole) {
        IntStream.range(0, NUM_TABLES).parallel().forEach(
            t -> {
                final Result<Record> fetch = dbConnectionPool.getConnectionToDb().selectFrom("t" + t).fetch();
                blackhole.consume(fetch);
            }
        );
    }

    @Benchmark
    public void withSingleConnectionFromPool(final Blackhole blackhole) {
        final DSLContext connectionToDb = dbConnectionPool.getConnectionToDb();
        IntStream.range(0, NUM_TABLES).forEach(
            t -> {
                final Result<Record> fetch = connectionToDb.selectFrom("t" + t).fetch();
                blackhole.consume(fetch);
            }
        );
    }

    @Benchmark
    public void withSingleConnectionFromPoolParallel(final Blackhole blackhole) {
        final DSLContext connectionToDb = dbConnectionPool.getConnectionToDb();
        IntStream.range(0, NUM_TABLES).parallel().forEach(
            t -> {
                final Result<Record> fetch = connectionToDb.selectFrom("t" + t).fetch();
                blackhole.consume(fetch);
            }
        );
    }
}
