/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import org.dcm.IRTable;

import javax.annotation.Nullable;

public final class TableRowGenerator extends Qualifier {
    private final IRTable table;

    public TableRowGenerator(final IRTable table) {
        this.table = table;
    }

    public IRTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "TableRowGenerator{" +
                "table=" + table.getName() +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitTableRowGenerator(this, context);
    }
}