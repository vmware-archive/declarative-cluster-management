/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.vmware.dcm.IRContext;
import com.vmware.dcm.IRTable;
import org.jooq.Record;
import org.jooq.Result;

import java.util.Map;

public interface IGeneratedBackend {
    Map<IRTable, Result<? extends Record>> solve(final IRContext context);
}
