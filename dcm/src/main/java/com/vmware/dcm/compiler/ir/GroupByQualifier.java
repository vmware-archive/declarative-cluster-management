/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

import java.util.List;
import java.util.stream.Collectors;

public final class GroupByQualifier extends Qualifier {
    private final List<Expr> exprs;

    public GroupByQualifier(final List<Expr> exprs) {
        this.exprs = exprs;
    }

    @Override
    public String toString() {
        return "GroupByQualifier{" +
                "groupBy=" + exprs +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final IRVisitor<T, C> visitor, final C context) {
        return visitor.visitGroupByQualifier(this, context);
    }

    public List<Expr> getGroupByExprs() {
        return exprs;
    }

    public List<ColumnIdentifier> getGroupByColumnIdentifiers() {
        return exprs.stream().filter(e -> e instanceof ColumnIdentifier)
                    .map(e -> (ColumnIdentifier) e)
                    .collect(Collectors.toList());
    }
}
