/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SubqueryExpression;

public class UsesAggregateFunctions extends DefaultTraversalVisitor<Void, Void> {
    private boolean found = false;

    @Override
    protected Void visitFunctionCall(final FunctionCall node, final Void context) {
        switch (node.getName().toString().toUpperCase()) {
            case "COUNT":
            case "SUM":
            case "MIN":
            case "MAX":
            case "ANY":
            case "ALL":
            case "ALL_DIFFERENT":
            case "ALL_EQUAL":
            case "INCREASING":
            case "CAPACITY_CONSTRAINT":
                found = true;
                break;
            default:
                found = false;
        }
        return super.visitFunctionCall(node, context);
    }

    @Override
    protected Void visitSubqueryExpression(final SubqueryExpression node, final Void context) {
        return null;
    }

    public boolean isFound() {
        return found;
    }
}
