/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.monoid;

public class SimpleVisitor extends MonoidVisitor<VoidType, VoidType> {

    @Override
    public VoidType visit(final Expr expr, final VoidType context) {
        return super.visit(expr, context);
    }

    public VoidType visit(final Expr expr) {
        return visit(expr, defaultReturn());
    }

    @Override
    protected final VoidType defaultReturn() {
        return VoidType.getAbsent();
    }
}
