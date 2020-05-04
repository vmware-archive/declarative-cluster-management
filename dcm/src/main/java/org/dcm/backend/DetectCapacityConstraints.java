/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidVisitor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

class DetectCapacityConstraints  extends MonoidVisitor<Void, Void>  {
    final List<MonoidFunction> nodes = new ArrayList<>();

    @Nullable
    @Override
    protected Void visitMonoidFunction(final MonoidFunction node, @Nullable final Void context) {
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
