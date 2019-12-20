/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidVisitor;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;


/**
 * A visitor that returns the set of accessed columns within a comprehension's scope, *without entering
 * sub-queries.
 */
class GetColumnIdentifiers extends MonoidVisitor<Void, Void> {
    private final LinkedHashSet<ColumnIdentifier> columnIdentifiers = new LinkedHashSet<>();

    @Nullable
    @Override
    protected Void visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Void context) {
        columnIdentifiers.add(node);
        return super.visitColumnIdentifier(node, context);
    }

    @Nullable
    @Override
    protected Void visitMonoidComprehension(final MonoidComprehension node, @Nullable final Void context) {
        return null;
    }

    @Nullable
    @Override
    protected Void visitGroupByComprehension(final GroupByComprehension node, @Nullable final Void context) {
        return null;
    }

    LinkedHashSet<ColumnIdentifier> getColumnIdentifiers() {
        return columnIdentifiers;
    }
}