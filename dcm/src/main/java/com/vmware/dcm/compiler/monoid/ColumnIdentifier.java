/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.monoid;

import com.vmware.dcm.IRColumn;

public class ColumnIdentifier extends Expr {
    private final String tableName;
    private final IRColumn field;
    // True if this ColumnIdentifier was accessed using a dereference (table.fieldName) in the source SQL view
    private final boolean fromDereferencedAccess;

    public ColumnIdentifier(final String table, final IRColumn field, final boolean fromDereferencedAccess) {
        this.tableName = table;
        this.field = field;
        this.fromDereferencedAccess = fromDereferencedAccess;
    }

    @Override
    public String toString() {
        return String.format("%s[<%s>]", field.getName(), tableName);
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, final C context) {
        return visitor.visitColumnIdentifier(this, context);
    }

    public IRColumn getField() {
        return field;
    }

    public String getTableName() {
        return field.getIRTable().getAliasedName();
    }

    /**
     * True if this column was referenced in the original view using a dereference. False otherwise.
     * XXX: Is only consumed by the MiniZinc backend, and is likely not required
     */
    public boolean fromDereferencedAccess() {
        return fromDereferencedAccess;
    }
}