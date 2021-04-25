/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

class JavaIdentifier {
    private final String varName;
    private final JavaType type;

    JavaIdentifier(final String varName, final JavaType type) {
        this.varName = varName;
        this.type = type;
    }

    public String varName() {
        return varName;
    }

    public JavaType type() {
        return type;
    }

    @Override
    public String toString() {
        return "JavaIdentifier{" +
                "varName='" + varName + '\'' +
                ", type=" + type +
                '}';
    }
}
