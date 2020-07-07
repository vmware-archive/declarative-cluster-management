/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler;

import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.SimpleVisitor;
import org.dcm.compiler.monoid.VoidType;

/**
 * If a query does not have any variables in it (say, in a predicate or a join key), then they return arrays
 * of type int. If they do access variables, then they're of type "var opt" int.
 */
public class UsesControllableFields extends SimpleVisitor {
    private boolean usesControllable = false;

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
}
