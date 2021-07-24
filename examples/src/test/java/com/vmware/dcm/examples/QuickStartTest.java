/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.examples;

import com.vmware.dcm.Model;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuickStartTest {

    @Test
    public void quickStart() {
        // Create an in-memory database and get a JOOQ connection to it
        final DSLContext conn = DSL.using("jdbc:h2:mem:");

        // A table representing some machines
        conn.execute("create table machines(id integer)");

        // A table representing tasks, that need to be assigned to machines by DCM.
        // To do so, create a variable column (prefixed by controllable__).
        conn.execute("create table tasks(task_id integer, controllable__worker_id integer, " +
                "foreign key (controllable__worker_id) references machines(id))");

        // Add four machines
        conn.execute("insert into machines values(1)");
        conn.execute("insert into machines values(3)");
        conn.execute("insert into machines values(5)");
        conn.execute("insert into machines values(8)");

        // Add two tasks
        conn.execute("insert into tasks values(1, null)");
        conn.execute("insert into tasks values(2, null)");

        // Time to specify a constraint! Just for fun, let's assign tasks to machines such that
        // the machine IDs sum up to 6.
        final String constraint = "create constraint example_constraint as " +
                "select * from tasks check sum(controllable__worker_id) = 6";

        // Create a DCM model using the database connection and the above constraint
        final Model model = Model.build(conn, List.of(constraint));

        // Solve and return the tasks table. The controllable__worker_id column will either be [1, 5] or [5, 1]
        final List<Integer> column = model.solve("TASKS")
                .map(e -> e.get("CONTROLLABLE__WORKER_ID", Integer.class));
        assertEquals(2, column.size());
        assertTrue(column.contains(1));
        assertTrue(column.contains(5));
    }
}