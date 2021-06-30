/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import org.jooq.Record;
import org.jooq.Result;

import java.util.Map;

public interface IGeneratedBackend {
    Map<String, Result<? extends Record>> solve(final Map<String, Result<? extends Record>> data);
}
