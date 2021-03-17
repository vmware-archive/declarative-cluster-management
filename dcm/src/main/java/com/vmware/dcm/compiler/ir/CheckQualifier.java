/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

public class CheckQualifier extends Qualifier {

    private final Expr expr;

    public CheckQualifier(final Expr expr) {
        this.expr = expr;
    }

    public Expr getExpr() {
        return expr;
    }

    @Override
    <T, C> T acceptVisitor(final IRVisitor<T, C> visitor, final C context) {
        return visitor.visitCheckExpression(this, context);
    }
}
