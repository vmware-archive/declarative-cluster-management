/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;


import org.jooq.Record;

import java.util.List;

/**
 * An interface used by the scheduler to bind pods to nodes, either in a real cluster or in an emulated environment
 */
public interface IPodToNodeBinder {

    void bindManyAsnc(final List<? extends Record> records);

    void unbindManyAsnc(final List<? extends Record> records);
}
