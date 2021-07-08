/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.SimpleVisitor;
import com.vmware.dcm.compiler.ir.VoidType;

/**
 * If a query does not have any variables in it (say, in a predicate or a join key), then they return arrays
 * of type int. If they do access variables, then they're of type "var opt" int.
 */
public class UsesControllableFields extends SimpleVisitor {
    private boolean usesControllable = false;

    private UsesControllableFields() {
        // Don't allow instantiation
    }

    @Override
    protected VoidType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        if (node.getField().isControllable()) {
            usesControllable = true;
        }
        return super.visitColumnIdentifier(node, context);
    }

    public boolean usesControllableFields() {
        return usesControllable;
    }

    public static boolean apply(final Expr expr) {
        final UsesControllableFields visitor = new UsesControllableFields();
        visitor.visit(expr);
        return visitor.usesControllableFields();
    }
}
