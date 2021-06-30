/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.vmware.dcm.IRContext;
import com.vmware.dcm.compiler.Program;
import com.vmware.dcm.compiler.ir.ListComprehension;
import org.jooq.Record;
import org.jooq.Result;

import java.util.List;
import java.util.Map;

public interface ISolverBackend {
    Map<String, Result<? extends Record>> runSolver(final Map<String, Result<? extends Record>> inputRecords);

    List<String> generateModelCode(final IRContext context, final Program<ListComprehension> irProgram);

    boolean needsGroupTables();
}
