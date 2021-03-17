/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.VoidType;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.SimpleVisitor;

import java.util.ArrayList;
import java.util.List;

class DetectCapacityConstraints extends SimpleVisitor {
    final List<FunctionCall> nodes = new ArrayList<>();

    @Override
    protected VoidType visitFunctionCall(final FunctionCall node, final VoidType context) {
        if (node.getFunction().equals(FunctionCall.Function.CAPACITY_CONSTRAINT)) {
            nodes.add(node);
        }
        return super.visitFunctionCall(node, context);
    }

    public static List<FunctionCall> apply(final Expr expr) {
        final DetectCapacityConstraints visitor = new DetectCapacityConstraints();
        visitor.visit(expr);
        return visitor.nodes;
    }
}
