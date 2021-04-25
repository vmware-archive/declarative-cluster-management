/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

class JavaExpression {
    private final String exprStr;
    private final JavaType type;

    JavaExpression(final String varName, final JavaType type) {
        this.exprStr = varName;
        this.type = type;
    }

    /**
     * Returns a Java expression (which might be a variable name or a function call)
     *
     * @return A String representing a variable name or a function call
     */
    public String asString() {
        return exprStr;
    }

    public JavaType type() {
        return type;
    }

    @Override
    public String toString() {
        return "JavaIdentifier{" +
                "exprStr='" + exprStr + '\'' +
                ", type=" + type +
                '}';
    }
}
