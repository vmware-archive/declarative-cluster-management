/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.compiler.monoid.ColumnIdentifier;
import com.vmware.dcm.compiler.monoid.Expr;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Resolves the tuple types for tables, views and group by tables, into their
 * corresponding Java types. For example, a table with an integer and a variable
 * column will have the Java parameter type recorded as a String, "Integer, IntVar".
 */
public class TupleMetadata {
    private static final String GENERATED_FIELD_NAME_PREFIX = "GenField";
    private final AtomicInteger generatedFieldNameCounter = new AtomicInteger(0);
    private final Map<String, Map<String, String>> tableToFieldToType = new HashMap<>();
    private final Map<String, String> viewTupleTypeParameters = new HashMap<>();
    private final Map<String, String> viewGroupByTupleTypeParameters = new HashMap<>();
    private final Map<String, Map<String, Integer>> viewToFieldIndex = new HashMap<>();

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

    /**
     * Updates the tracked index for a field within a loop's result set
     *
     * @param viewName the view within which this expression is being visited
     * @param exprs The expressions to create a field for
     * @return A string representing the fields being accessed
     */
    <T extends Expr> String computeViewIndices(final String viewName, final List<T> exprs) {
        final AtomicInteger counter = new AtomicInteger(0);
        return exprs.stream().map(argument -> {
                final String fieldName = argument.getAlias().orElseGet(() -> {
                        if (argument instanceof ColumnIdentifier) {
                            return ((ColumnIdentifier) argument).getField().getName();
                        } else {
                            return GENERATED_FIELD_NAME_PREFIX + generatedFieldNameCounter.getAndIncrement();
                        }
                    }
                )
                        .toUpperCase(Locale.US);
                viewToFieldIndex.computeIfAbsent(viewName.toUpperCase(Locale.US), (k) -> new HashMap<>())
                        .compute(fieldName, (k, v) -> counter.getAndIncrement());
                return fieldName;
            }
        ).collect(Collectors.joining(",\n    "));
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

    boolean canBeAccessedWithViewIndices(final String tableName) {
        return viewToFieldIndex.containsKey(tableName);
    }

    // when duplicates appear for viewToFieldIndex, we increment the fieldIndex counter but do not add a new
    // entry. This means that the highest fieldIndex (and not the size of the map) is equal to tuple size.
    // The indices are 0-indexed.
    int getTupleSize(final String tableName) {
        return Collections.max(viewToFieldIndex.get(tableName.toUpperCase(Locale.US)).values()) + 1;
    }

    int getViewIndexForField(final String tableName, final String columnName) {
        return Objects.requireNonNull(viewToFieldIndex.get(tableName).get(columnName));
    }

    <T extends Expr> String generateTupleGenericParameters(final List<T> exprs) {
        return exprs.stream().map(this::inferType)
                .collect(Collectors.joining(", "));
    }

    String inferType(final Expr expr) {
        return InferType.forExpr(expr, viewTupleTypeParameters);
    }
}