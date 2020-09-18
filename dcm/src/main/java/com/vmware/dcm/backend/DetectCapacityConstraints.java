/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.vmware.dcm.compiler.monoid.MonoidFunction;
import com.vmware.dcm.compiler.monoid.VoidType;
import com.vmware.dcm.compiler.monoid.Expr;
import com.vmware.dcm.compiler.monoid.SimpleVisitor;

import java.util.ArrayList;
import java.util.List;

class DetectCapacityConstraints extends SimpleVisitor {
    final List<MonoidFunction> nodes = new ArrayList<>();

    @Override
    protected VoidType visitMonoidFunction(final MonoidFunction node, final VoidType context) {
        if (node.getFunction().equals(MonoidFunction.Function.CAPACITY_CONSTRAINT)) {
            nodes.add(node);
        }
        return super.visitMonoidFunction(node, context);
    }

    public static List<MonoidFunction> apply(final Expr expr) {
        final DetectCapacityConstraints visitor = new DetectCapacityConstraints();
        visitor.visit(expr);
        return visitor.nodes;
    }
}
