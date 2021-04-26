/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.Expr;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Tracks metadata about tuples. Specifically, it tracks the tuple types for tables, views and group by tables,
 * and resolves them into their corresponding Java types. For example, a table with an integer and a variable
 * column will have the Java parameter type recorded as a String, "Integer, IntVar".
 * For tuples created by views we create, it also tracks the indices of fields within the tuple.
 */
public class TupleMetadata {
    private final Map<String, Map<String, JavaType>> tableToFieldToType = new HashMap<>();
    private final Map<String, Map<String, Integer>> tableToFieldIndex = new HashMap<>();
    private final Map<String, Map<String, Integer>> viewToFieldIndex = new HashMap<>();
    private final Map<String, JavaTypeList> viewTupleTypeParameters = new HashMap<>();

    String computeTableTupleType(final IRTable table) {
        Preconditions.checkArgument(!tableToFieldToType.containsKey(table.getAliasedName()));
        Preconditions.checkArgument(!tableToFieldIndex.containsKey(table.getAliasedName()));
        final AtomicInteger fieldIndex = new AtomicInteger(0);
        return table.getIRColumns().entrySet().stream()
                .map(e -> {
                        final JavaType retVal = InferType.typeFromColumn(e.getValue());
                        tableToFieldToType.computeIfAbsent(table.getAliasedName(), (k) -> new HashMap<>())
                                          .putIfAbsent(e.getKey(), retVal);
                        tableToFieldIndex.computeIfAbsent(table.getAliasedName(),  (k) -> new HashMap<>())
                                          .putIfAbsent(e.getKey(), fieldIndex.getAndIncrement());
                        return retVal.typeString();
                    }
                ).collect(Collectors.joining(", "));
    }

    <T extends Expr> JavaTypeList computeViewTupleType(final String viewName, final List<T> exprs) {
        Preconditions.checkArgument(!viewTupleTypeParameters.containsKey(viewName), viewName + " " + exprs);
        return viewTupleTypeParameters.compute(viewName, (k, v) -> computeTupleGenericParameters(exprs));
    }

    /**
     * Updates the tracked index for a field within a loop's result set
     *
     * @param viewName the view within which this expression is being visited
     * @param exprs The expressions to create a field for
     */
    <T extends Expr> void recordFieldIndices(final String viewName, final List<T> exprs) {
        final AtomicInteger counter = new AtomicInteger(0);
        exprs.forEach(argument -> {
                final String fieldName = argument.getAlias().orElseGet(() -> {
                        if (argument instanceof ColumnIdentifier) {
                            return ((ColumnIdentifier) argument).getField().getName();
                        }
                        throw new ModelException("Non-column fields need an alias: " + argument);
                    }
                ).toUpperCase(Locale.US);
                viewToFieldIndex.computeIfAbsent(viewName.toUpperCase(Locale.US), (k) -> new HashMap<>())
                        .compute(fieldName, (k, v) -> counter.getAndIncrement());
            }
        );
    }

    JavaType getTypeForField(final IRTable table, final IRColumn column) {
        return Objects.requireNonNull(tableToFieldToType.get(table.getName()).get(column.getName()));
    }

    JavaType getTypeForField(final String tableName, final String columnName) {
        return Objects.requireNonNull(tableToFieldToType.get(tableName).get(columnName));
    }

    int getFieldIndexInTable(final String tableName, final String columnName) {
        return Objects.requireNonNull(tableToFieldIndex.get(tableName).get(columnName));
    }

    int getFieldIndexInView(final String tableName, final String columnName) {
        return Objects.requireNonNull(viewToFieldIndex.get(tableName).get(columnName));
    }

    boolean canBeAccessedWithViewIndices(final String tableName) {
        return viewToFieldIndex.containsKey(tableName);
    }

    <T extends Expr> JavaTypeList computeTupleGenericParameters(final List<T> exprs) {
        return new JavaTypeList(exprs.stream().map(this::inferType).collect(Collectors.toList()));
    }

    JavaType inferType(final Expr expr) {
        return InferType.forExpr(expr, viewTupleTypeParameters);
    }
}