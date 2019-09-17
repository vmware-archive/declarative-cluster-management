/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.examples;

import com.google.common.base.Splitter;
import com.google.common.io.Files;
import org.dcm.Model;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.sql.DriverManager.getConnection;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;


class LoadBalance {
    private static final String PHYSICAL_MACHINES_TABLE = "PHYSICAL_MACHINE";
    private static final String VIRTUAL_MACHINES_TABLE = "VIRTUAL_MACHINE";
    private final DSLContext conn;
    private final Model model;

    LoadBalance(final List<String> constraints) {
        conn = setup();
        final File tempDir = Files.createTempDir();
        final File modelFile = new File(tempDir.getPath() + "/load_balance_model.mzn");
        final File dataFile = new File(tempDir.getPath() + "/load_balance_data.dzn");
        model = Model.buildModel(conn, constraints, modelFile, dataFile);
    }

    void run() {
        model.updateData();
        final Map<String, Result<? extends Record>> virtualMachineTableNew
                = model.solveModelWithoutTableUpdates(Collections.singleton(VIRTUAL_MACHINES_TABLE));
        System.out.println(virtualMachineTableNew.values());
    }

    void addNode(final String nodeName, final int cpuCapacity, final int memoryCapacity) {
        conn.insertInto(table(PHYSICAL_MACHINES_TABLE))
                .values(nodeName, cpuCapacity, memoryCapacity).execute();
    }

    void addVm(final String vmName, final int cpu, final int memory) {
        conn.insertInto(table(VIRTUAL_MACHINES_TABLE))
            .values(vmName, cpu, memory, null).execute();
    }

    /*
     * Sets up an in-memory Apache Derby database.
     */
    private DSLContext setup() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // The following block ensures we always drop the database between invocations of this method
            try {
                final String dropUrl = "jdbc:derby:memory:test;drop=true";
                getConnection(dropUrl, properties).close();
            } catch (final SQLException e) {
                // We could not drop a database because it was never created. Move on.
            }
            // Create a fresh database
            final String connectionURL = "jdbc:derby:memory:test;create=true";
            final Connection conn = getConnection(connectionURL, properties);
            final DSLContext using = using(conn, SQLDialect.DERBY);
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