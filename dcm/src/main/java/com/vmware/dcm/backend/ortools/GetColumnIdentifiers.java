/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.SimpleVisitor;
import com.vmware.dcm.compiler.ir.VoidType;

import java.util.LinkedHashSet;


/**
 * A visitor that returns the set of accessed columns within a comprehension's scope, without entering
 * sub-queries.
 */
class GetColumnIdentifiers extends SimpleVisitor {
    private final LinkedHashSet<ColumnIdentifier> columnIdentifiers = new LinkedHashSet<>();
    private final boolean visitInnerComprehensions;

    GetColumnIdentifiers(final boolean visitInnerComprehensions) {
        this.visitInnerComprehensions = visitInnerComprehensions;
    }

    @Override
    protected VoidType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        columnIdentifiers.add(node);
        return defaultReturn();
    }

    @Override
    protected VoidType visitListComprehension(final ListComprehension node, final VoidType context) {
        if (visitInnerComprehensions) {
            super.visitListComprehension(node, context);
        }
        return defaultReturn();
    }

    @Override
    protected VoidType visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
        if (visitInnerComprehensions) {
            super.visitGroupByComprehension(node, context);
        }
        return defaultReturn();
    }

    LinkedHashSet<ColumnIdentifier> getColumnIdentifiers() {
        return columnIdentifiers;
    }
}