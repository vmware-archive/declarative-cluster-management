/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.MonoidVisitor;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;

class GetColumnIdentifiers extends MonoidVisitor<Void, Void> {
    private final LinkedHashSet<ColumnIdentifier> columnIdentifiers = new LinkedHashSet<>();

    @Nullable
    @Override
    protected Void visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Void context) {
        columnIdentifiers.add(node);
        return super.visitColumnIdentifier(node, context);
    }

    LinkedHashSet<ColumnIdentifier> getColumnIdentifiers() {
        return columnIdentifiers;
    }
}