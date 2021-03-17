/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.IRPrimaryKey;

import java.util.Optional;

public final class TableRowGenerator extends Qualifier {
    private final IRTable table;

    public TableRowGenerator(final IRTable table) {
        this.table = table;
    }

    public IRTable getTable() {
        return table;
    }

    public Optional<IRColumn> getUniquePrimaryKeyColumn() {
        return table.getPrimaryKey().map(IRPrimaryKey::getPrimaryKeyFields)
                .filter(l -> l.size() == 1)
                .map(l -> l.get(0));
    }

    @Override
    public String toString() {
        return "TableRowGenerator{" +
                "table=" + table.getName() +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final IRVisitor<T, C> visitor, final C context) {
        return visitor.visitTableRowGenerator(this, context);
    }
}