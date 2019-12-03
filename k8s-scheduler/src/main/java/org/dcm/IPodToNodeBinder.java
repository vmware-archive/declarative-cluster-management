/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

public interface IPodToNodeBinder {
    void bindOne(final String namespace, final String podName, final String nodeName);
}
