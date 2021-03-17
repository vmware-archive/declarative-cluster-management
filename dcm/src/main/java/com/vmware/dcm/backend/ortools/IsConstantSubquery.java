/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.TableRowGenerator;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Evaluates whether a sub-query (and its inner sub-queries etc.) can be treated as a constant expression.
 */
public class IsConstantSubquery {

    static boolean apply(final ListComprehension expr) {
        final GetColumnIdentifiers visitor = new GetColumnIdentifiers(true);
        if (expr instanceof GroupByComprehension) {
            final ListComprehension comprehension = ((GroupByComprehension) expr).getComprehension();
            comprehension.getHead().getSelectExprs().forEach(visitor::visit);
            comprehension.getQualifiers().forEach(visitor::visit);
        } else {
            expr.getHead().getSelectExprs().forEach(visitor::visit);
            expr.getQualifiers().forEach(visitor::visit);
        }
        final LinkedHashSet<ColumnIdentifier> columnIdentifiers = visitor.getColumnIdentifiers();

        final Set<String> accessedTables = expr.getQualifiers()
                .stream().filter(q -> q instanceof TableRowGenerator)
                .map(e -> ((TableRowGenerator ) e).getTable().getAliasedName())
                .collect(Collectors.toSet());
        return columnIdentifiers.stream().allMatch(
                ci -> !ci.getField().isControllable() && accessedTables.contains(ci.getTableName())
        );
    }
}
