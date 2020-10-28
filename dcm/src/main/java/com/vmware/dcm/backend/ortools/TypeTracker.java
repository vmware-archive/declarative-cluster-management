/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.compiler.monoid.Expr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Resolves the tuple types for tables, views and group by tables, into their
 * corresponding Java types. For example, a table with an integer and a variable
 * column will have the Java parameter type recorded as a String, "Integer, IntVar".
 */
public class TypeTracker {
    private final Map<String, Map<String, String>> tableToFieldToType = new HashMap<>();
    private final Map<String, String> viewTupleTypeParameters = new HashMap<>();
    private final Map<String, String> viewGroupByTupleTypeParameters = new HashMap<>();

    String computeTableTupleType(final IRTable table) {
        Preconditions.checkArgument(!tableToFieldToType.containsKey(table.getAliasedName()));
        return table.getIRColumns().entrySet().stream()
                .map(e -> {
                            final String retVal = InferType.typeStringFromColumn(e.getValue());
                            // Tracks the type of each field.
                            tableToFieldToType.computeIfAbsent(table.getAliasedName(), (k) -> new HashMap<>())
                                    .putIfAbsent(e.getKey(), retVal);
                            return retVal;
                        }
                ).collect(Collectors.joining(", "));
    }

    <T extends Expr> String computeGroupByTupleType(final String viewName, final List<T> exprs) {
        Preconditions.checkArgument(!viewGroupByTupleTypeParameters.containsKey(viewName));
        return viewGroupByTupleTypeParameters.compute(viewName, (k, v) -> generateTupleGenericParameters(exprs));
    }

    <T extends Expr> String computeViewTupleType(final String viewName, final List<T> exprs) {
        Preconditions.checkArgument(!viewTupleTypeParameters.containsKey(viewName));
        return viewTupleTypeParameters.compute(viewName, (k, v) -> generateTupleGenericParameters(exprs));
    }


    String getGroupByTupleType(final String viewName) {
        return viewGroupByTupleTypeParameters.get(viewName);
    }

    String getViewTupleType(final String viewName) {
        return viewTupleTypeParameters.get(viewName);
    }

    String getTypeForField(final IRTable table, final IRColumn column) {
        return Objects.requireNonNull(tableToFieldToType.get(table.getName()).get(column.getName()));
    }

    String getTypeForField(final String tableName, final String columnName) {
        return Objects.requireNonNull(tableToFieldToType.get(tableName).get(columnName));
    }

    <T extends Expr> String generateTupleGenericParameters(final List<T> exprs) {
        return exprs.stream().map(this::inferType)
                .collect(Collectors.joining(", "));
    }

    String inferType(final Expr expr) {
        return InferType.forExpr(expr, viewTupleTypeParameters);
    }
}