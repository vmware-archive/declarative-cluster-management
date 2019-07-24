/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.IRContext;
import org.dcm.IRTable;
import org.jooq.Record;
import org.jooq.Result;

import java.util.Map;

public interface IGeneratedBackend {
    Map<IRTable, Result<? extends Record>> solve(final IRContext context);
}
