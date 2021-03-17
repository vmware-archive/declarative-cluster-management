/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

public final class VoidType {
    private static final VoidType ABSENT = new VoidType();

    private VoidType() {
        // This is empty to prevent initialization
    }

    public static VoidType getAbsent() {
        return ABSENT;
    }
}