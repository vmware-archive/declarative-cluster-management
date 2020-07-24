/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.examples;

import com.google.common.base.Splitter;
import org.dcm.Model;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.sql.DriverManager.getConnection;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;


/**
 * A simple class to highlight DCM's capabilities. Loading this class creates a database schema
 * that according to resources/schema.sql. It creates two tables, one for physical machines, and
 * one for virtual machines. A variable column in the virtual machines table tracks the assignment
 * of each virtual machine to a physical machine.
 *
 * The class is currently driven by tests written in LoadBalanceTest.
 */
class LoadBalance {
    private static final String PHYSICAL_MACHINES_TABLE = "PHYSICAL_MACHINE";
    private static final String VIRTUAL_MACHINES_TABLE = "VIRTUAL_MACHINE";
    private final DSLContext conn;
    private final Model model;

    LoadBalance(final List<String> constraints) {
        conn = setup();
        model = Model.build(conn, constraints);
    }

    /**
     * Add a physical machine to the inventory.
     *
     * @param nodeName machine name
     * @param cpuCapacity CPU capacity
     * @param memoryCapacity Memory capacity
     */
    void addPhysicalMachine(final String nodeName, final int cpuCapacity, final int memoryCapacity) {
        conn.insertInto(table(PHYSICAL_MACHINES_TABLE))
                .values(nodeName, cpuCapacity, memoryCapacity).execute();
    }


    /**
     * Add a virtual machine to the inventory.
     *
     * @param vmName name
     * @param cpu CPU demand
     * @param memory Memory demand
     */
    void addVirtualMachine(final String vmName, final int cpu, final int memory) {
        conn.insertInto(table(VIRTUAL_MACHINES_TABLE))
                .values(vmName, cpu, memory, null).execute();
        System.out.println(conn.selectFrom(table(PHYSICAL_MACHINES_TABLE)).fetch());
        System.out.println(conn.selectFrom(table(VIRTUAL_MACHINES_TABLE)).fetch());
    }
    /**
     * Invoke to solve for a given database (physical and virtual machine tables).
     *
     * @return The new virtual machine table after the solver identifies a new placement.
     */
    Result<? extends Record> run() {
        // Pull the latest state from the DB
        model.updateData();

        // Run the solver and return the virtual machines table with solver-identified values for the
        // controllable__physical_machines column
        return model.solve(VIRTUAL_MACHINES_TABLE);
    }


    /*
     * Sets up an in-memory Apache Derby database.
     */
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
            final InputStream resourceAsStream = this.getClass().getResourceAsStream("/schema.sql");
            final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(resourceAsStream, Charset.forName("UTF8")));
            final String schemaAsString = reader
                    .lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);
            reader.close();
            semiColonSeparated.forEach(using::execute);
            return using;
        } catch (final SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}