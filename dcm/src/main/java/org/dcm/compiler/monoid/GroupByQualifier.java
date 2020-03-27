/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;
import java.util.List;

public final class GroupByQualifier extends Qualifier {
    private final List<ColumnIdentifier> columnIdentifiers;

    public GroupByQualifier(final List<ColumnIdentifier> columnIdentifiers) {
        this.columnIdentifiers = columnIdentifiers;
    }

    @Override
    public String toString() {
        return "GroupByQualifier{" +
                "groupBy=" + columnIdentifiers +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitGroupByQualifier(this, context);
    }

    public List<ColumnIdentifier> getColumnIdentifiers() {
        return columnIdentifiers;
    }
}
