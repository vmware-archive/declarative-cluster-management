/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;


import org.jooq.Record;
import org.jooq.Result;

/**
 * An interface used by the scheduler to bind pods to nodes, either in a real cluster or in an emulated environment
 */
public interface IPodToNodeBinder {
    void bindManyAsnc(final Result<? extends Record> records);
}
