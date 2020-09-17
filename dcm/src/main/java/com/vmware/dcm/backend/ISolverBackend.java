/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.vmware.dcm.IRContext;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.util.List;
import java.util.Map;

public interface ISolverBackend {
    Map<IRTable, Result<? extends Record>> runSolver(final DSLContext dbCtx,
                                                     final Map<String, IRTable> irTables);

    List<String> generateModelCode(final IRContext context,
                                   final Map<String, MonoidComprehension> nonConstraintViews,
                                   final Map<String, MonoidComprehension> constraintViews,
                                   final Map<String, MonoidComprehension> objectiveFunctions);

    List<String> generateDataCode(final IRContext context);

    boolean needsGroupTables();
}
