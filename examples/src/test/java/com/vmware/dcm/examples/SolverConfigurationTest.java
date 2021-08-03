/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.examples;

import com.vmware.dcm.Model;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.jooq.impl.DSL.using;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SolverConfigurationTest {
    @Test
    public void backendInitializationExample() {
        final DSLContext conn = using("jdbc:h2:mem:");
        conn.execute("CREATE TABLE t1(controllable__c1 integer)");
        conn.execute("insert into t1 values(null)");
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                .setNumThreads(1)
                .setPrintDiagnostics(true)
                .setMaxTimeInSeconds(5).build();
        final Model build = Model.build(conn, orToolsSolver,
                List.of("CREATE CONSTRAINT C1 AS SELECT * FROM t1 CHECK controllable__c1 = 10"));
        assertNotNull(build.solve("T1"));
    }
}
