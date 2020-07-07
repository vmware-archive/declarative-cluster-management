/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import org.dcm.IRColumn;

public class ColumnIdentifier extends Expr {
    private final String tableName;
    private final IRColumn field;
    private final boolean fromGroupByWithDereference;

    public ColumnIdentifier(final String table, final IRColumn field, final boolean fromGroupByWithDereference) {
        this.tableName = table;
        this.field = field;
        this.fromGroupByWithDereference = fromGroupByWithDereference;
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
     * true if this column was referenced in a group by using a dereference. False otherwise.
     */
    public boolean fromGroupByWithDereference() {
        return fromGroupByWithDereference;
    }
}