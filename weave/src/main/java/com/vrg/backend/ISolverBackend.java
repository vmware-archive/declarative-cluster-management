/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg.backend;

import com.vrg.IRContext;
import com.vrg.IRTable;
import com.vrg.compiler.monoid.MonoidComprehension;
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
}
