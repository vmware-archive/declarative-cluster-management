/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.SimpleVisitor;
import org.dcm.compiler.monoid.VoidType;

import java.util.LinkedHashSet;


/**
 * A visitor that returns the set of accessed columns within a comprehension's scope, *without entering
 * sub-queries.
 */
class GetColumnIdentifiers extends SimpleVisitor {
    private final LinkedHashSet<ColumnIdentifier> columnIdentifiers = new LinkedHashSet<>();

    @Override
    protected VoidType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        columnIdentifiers.add(node);
        return defaultReturn();
    }

    @Override
    protected VoidType visitMonoidComprehension(final MonoidComprehension node, final VoidType context) {
        return defaultReturn();
    }

    @Override
    protected VoidType visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
        return defaultReturn();
    }

    LinkedHashSet<ColumnIdentifier> getColumnIdentifiers() {
        return columnIdentifiers;
    }
}