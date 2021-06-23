/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.Head;
import com.vmware.dcm.compiler.ir.IRVisitor;
import com.vmware.dcm.compiler.ir.IsNotNullPredicate;
import com.vmware.dcm.compiler.ir.IsNullPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.UnaryOperator;
import com.vmware.dcm.compiler.ir.VoidType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * TODO: we likely don't need the split between tables/views here
 */
public class TupleMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(TupleMetadata.class);
    private final Map<String, Map<String, JavaType>> tableToFieldToType = new HashMap<>();
    private final Map<String, Map<String, Integer>> tableToFieldIndex = new HashMap<>();
    private final Map<String, Map<String, Integer>> viewToFieldIndex = new HashMap<>();
    private final Map<String, JavaTypeList> viewTupleTypeParameters = new HashMap<>();
    private final InferType inferType = new InferType();

    String computeTableTupleType(final IRTable table) {
        Preconditions.checkArgument(!tableToFieldToType.containsKey(table.getAliasedName()));
        Preconditions.checkArgument(!tableToFieldIndex.containsKey(table.getAliasedName()));
        final AtomicInteger fieldIndex = new AtomicInteger(0);
        return table.getIRColumns().entrySet().stream()
                .map(e -> {
                        final JavaType retVal = inferType.typeFromColumn(e.getValue());
                        tableToFieldToType.computeIfAbsent(table.getAliasedName(), (k) -> new HashMap<>())
                                          .putIfAbsent(e.getKey(), retVal);
                        tableToFieldIndex.computeIfAbsent(table.getAliasedName(),  (k) -> new HashMap<>())
                                          .putIfAbsent(e.getKey(), fieldIndex.getAndIncrement());
                        return retVal.typeString();
                    }
                ).collect(Collectors.joining(", "));
    }

    <T extends Expr> JavaTypeList computeViewTupleType(final String viewName, final List<T> exprs) {
        final String upperCased = viewName.toUpperCase(Locale.US);
        Preconditions.checkArgument(!viewTupleTypeParameters.containsKey(upperCased), upperCased + " " + exprs);
        return viewTupleTypeParameters.compute(upperCased, (k, v) -> computeTupleGenericParameters(exprs));
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

    JavaType inferType(final IRColumn column) {
        return inferType.typeFromColumn(column);
    }

    JavaType inferType(final Expr expr) {
        return inferType.visit(expr);
    }

    private class InferType extends IRVisitor<JavaType, VoidType> {
        public JavaType visit(final Expr expr) {
            return super.visit(expr, VoidType.getAbsent());
        }

        @Override
        protected JavaType visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final VoidType context) {
            final JavaType leftType = visit(node.getLeft());
            final JavaType rightType = visit(node.getRight());
            if (leftType == JavaType.IntVar || rightType == JavaType.IntVar) {
                return JavaType.IntVar;
            }
            switch (node.getOperator()) {
                case EQUAL:
                case NOT_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case IN:
                case OR:
                case AND:
                case CONTAINS:
                    return JavaType.Boolean;
                case ADD:
                case SUBTRACT:
                case MULTIPLY:
                case DIVIDE:
                case MODULUS:
                    return JavaType.Integer;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected JavaType visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
            final Head head = node.getComprehension().getHead();
            if (head.getSelectExprs().size() == 1) {
                final JavaType type = visit(head.getSelectExprs().get(0), context);
                LOG.warn("Returning type of sub-query {} as {}", node, type);
                return type;
            }
            throw new UnsupportedOperationException("Do not know type of subquery");
        }

        @Override
        protected JavaType visitListComprehension(final ListComprehension node, final VoidType context) {
            final Head head = node.getHead();
            if (head.getSelectExprs().size() == 1) {
                final JavaType type = visit(head.getSelectExprs().get(0), context);
                LOG.warn("Returning type of sub-query {} as {}", node, type);
                return type;
            }
            throw new UnsupportedOperationException("Do not know type of subquery");
        }

        @Override
        protected JavaType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
            return typeFromColumn(node);
        }

        @Override
        protected JavaType visitFunctionCall(final FunctionCall node, final VoidType context) {
            if (node.getFunction().equals(FunctionCall.Function.CAPACITY_CONSTRAINT)) {
                return JavaType.IntVar;
            }
            Preconditions.checkArgument(node.getArgument().size() == 1);
            return visit(node.getArgument().get(0), context);
        }

        @Override
        protected JavaType visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
            // TODO: This is incomplete. It can be boolean if node.getArgument() is const.
            return visit(node.getArgument(), context) == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
        }

        @Override
        protected JavaType visitIsNullPredicate(final IsNullPredicate node, final VoidType context) {
            return visit(node.getArgument(), context) == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
        }

        @Override
        protected JavaType visitIsNotNullPredicate(final IsNotNullPredicate node, final VoidType context) {
            return visit(node.getArgument(), context) == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
        }

        @Override
        protected JavaType visitUnaryOperator(final UnaryOperator node, final VoidType context) {
            final JavaType type = visit(node.getArgument(), context);
            switch (node.getOperator()) {
                case NOT:
                    return type == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
                case MINUS:
                case PLUS:
                    return type;
                default:
                    throw new IllegalArgumentException(node.toString());
            }
        }

        @Override
        protected JavaType visitLiteral(final Literal node, final VoidType context) {
            if (node.getValue() instanceof String) {
                return JavaType.String;
            } else if (node.getValue() instanceof Integer) {
                return JavaType.Integer;
            } else if (node.getValue() instanceof Boolean) {
                return JavaType.Boolean;
            } else if (node.getValue() instanceof Long) {
                return JavaType.Long;
            }
            return super.visitLiteral(node, context);
        }

        private JavaType typeFromColumn(final ColumnIdentifier node) {
            if (node.getField().isControllable()) {
                return JavaType.IntVar;
            }
            if (viewTupleTypeParameters.containsKey(node.getTableName())) {
                final JavaTypeList typeList = viewTupleTypeParameters.get(node.getTableName());
                final int index = viewToFieldIndex.get(node.getTableName()).get(node.getField().getName());
                return typeList.get(index);
            }
            return typeFromColumn(node.getField());
        }

        JavaType typeFromColumn(final IRColumn column) {
            switch (column.getType()) {
                case STRING:
                    return JavaType.String;
                case BOOL:
                    return JavaType.Boolean;
                case LONG:
                    return JavaType.Long;
                case INT:
                    return JavaType.Integer;
                case FLOAT:
                    return JavaType.Float;
                case ARRAY:
                    return JavaType.ObjectArray;
                default:
                    throw new IllegalArgumentException(column.toString());
            }
        }
    }
}