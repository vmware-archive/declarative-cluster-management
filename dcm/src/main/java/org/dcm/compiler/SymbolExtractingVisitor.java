/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler;


import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;

import java.util.Locale;

/**
 * Extract all the necessary symbols (tables, fields, views) from a single CREATE VIEW statement
 */
class SymbolExtractingVisitor extends DefaultTraversalVisitor<Void, ReferencedSymbols> {
    @Override
    /**
     * Parses CREATE VIEW statements by splitting constraint views (prefixed with CONSTRAINT__) from
     * non-constraint views.
     */
    protected Void visitCreateView(final CreateView view, final ReferencedSymbols context) {
        final String viewName = view.getName().toString();
        if (viewName.toLowerCase(Locale.US).startsWith("constraint_")) {
            context.getConstraintViews().put(viewName, view.getQuery());
        }
        else if (viewName.toLowerCase(Locale.US).startsWith("objective_")) {
            context.getObjectiveFunctionViews().put(viewName, view.getQuery());
        }
        else {
            context.getNonConstraintViews().put(viewName, view.getQuery());
        }
        return super.visitCreateView(view, context);
    }
}