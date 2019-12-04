/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;


/**
 * An interface used by the scheduler to bind pods to nodes, either in a real cluster or in an emulated environment
 */
public interface IPodToNodeBinder {
    void bindOne(final String namespace, final String podName, final String nodeName);
}
