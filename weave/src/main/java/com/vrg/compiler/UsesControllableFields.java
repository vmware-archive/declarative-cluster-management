package com.vrg.compiler;

import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.MonoidVisitor;

/**
 * If a query does not have any variables in it (say, in a predicate or a join key), then they return arrays
 * of type int. If they do access variables, then they're of type "var opt" int.
 */
public class UsesControllableFields extends MonoidVisitor {
    private boolean usesControllable = false;

    @Override
    protected void visitColumnIdentifier(final ColumnIdentifier node) {
        if (node.getField().isControllable()) {
            usesControllable = true;
        }
        super.visitColumnIdentifier(node);
    }

    public boolean usesControllableFields() {
        return usesControllable;
    }
}
