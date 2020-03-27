/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import org.dcm.IRColumn;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.ExistsPredicate;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.Head;
import org.dcm.compiler.monoid.IsNotNullPredicate;
import org.dcm.compiler.monoid.IsNullPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.MonoidVisitor;
import org.dcm.compiler.monoid.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

class InferType extends MonoidVisitor<String, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(InferType.class);
    private final Map<String, String> viewTupleTypeParameters;

    InferType(final Map<String, String> viewTupleTypeParameters) {
        this.viewTupleTypeParameters = viewTupleTypeParameters;
    }

    @Nullable
    @Override
    protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                  @Nullable final Void context) {
        final String leftType = visit(node.getLeft());
        final String rightType = visit(node.getRight());
        Preconditions.checkNotNull(leftType);
        Preconditions.checkNotNull(rightType, "type was null: " + node.getRight());
        if (leftType.equals("IntVar") || rightType.equals("IntVar")) {
            return "IntVar";
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
                return "Boolean";
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULUS:
                return "Integer";
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Nullable
    @Override
    protected String visitGroupByComprehension(final GroupByComprehension node, @Nullable final Void context) {
        final Head head = node.getComprehension().getHead();
        if (head != null && head.getSelectExprs().size() == 1) {
            final String type = visit(head.getSelectExprs().get(0), context);
            LOG.warn("Returning type of sub-query {} as {}", node, type);
            return type;
        }
        throw new UnsupportedOperationException("Do not know type of subquery");
    }

    @Nullable
    @Override
    protected String visitMonoidComprehension(final MonoidComprehension node, @Nullable final Void context) {
        final Head head = node.getHead();
        if (head != null && head.getSelectExprs().size() == 1) {
            final String type = visit(head.getSelectExprs().get(0), context);
            LOG.warn("Returning type of sub-query {} as {}", node, type);
            return type;
        }
        throw new UnsupportedOperationException("Do not know type of subquery");
    }

    @Nullable
    @Override
    protected String visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Void context) {
        return typeStringFromColumn(node);
    }

    @Nullable
    @Override
    protected String visitMonoidFunction(final MonoidFunction node, @Nullable final Void context) {
        return visit(node.getArgument(), context);
    }

    @Nullable
    @Override
    protected String visitExistsPredicate(final ExistsPredicate node, @Nullable final Void context) {
        // TODO: This is incomplete. It can be boolean if node.getArgument() is const.
        return "IntVar";
    }

    @Nullable
    @Override
    protected String visitIsNullPredicate(final IsNullPredicate node, @Nullable final Void context) {
        return Objects.requireNonNull(visit(node.getArgument(), context)).equals("IntVar") ? "IntVar" : "Boolean";
    }

    @Nullable
    @Override
    protected String visitIsNotNullPredicate(final IsNotNullPredicate node, @Nullable final Void context) {
        return Objects.requireNonNull(visit(node.getArgument(), context)).equals("IntVar") ? "IntVar" : "Boolean";
    }

    @Nullable
    @Override
    protected String visitUnaryOperator(final UnaryOperator node, @Nullable final Void context) {
        final String type = Objects.requireNonNull(visit(node.getArgument(), context));
        switch (node.getOperator()) {
            case NOT:
                return type.equals("IntVar") ? "IntVar" : "Boolean";
            case MINUS:
            case PLUS:
                return type.equals("IntVar") ? "IntVar" : "Integer";
            default:
                throw new IllegalArgumentException(node.toString());
        }
    }

    @Nullable
    @Override
    protected String visitMonoidLiteral(final MonoidLiteral node, @Nullable final Void context) {
        if (node.getValue() instanceof String) {
            return "String";
        } else if (node.getValue() instanceof Integer) {
            return "Integer";
        } else if (node.getValue() instanceof Boolean) {
            return "Boolean";
        } else if (node.getValue() instanceof Long) {
            return "Integer";
        }
        return super.visitMonoidLiteral(node, context);
    }

    private  String typeStringFromColumn(final ColumnIdentifier node) {
        if (node.getField().isControllable()) {
            return "IntVar";
        }
        final String convertedName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, node.getTableName());
        if (viewTupleTypeParameters.containsKey(convertedName) &&
                viewTupleTypeParameters.get(convertedName).contains("IntVar")) {
            LOG.warn("Inferring type for column {} as IntVar", node);
            return "IntVar";
        }
        return typeStringFromColumn(node.getField());
    }

    static String typeStringFromColumn(final IRColumn column) {
        switch (column.getType()) {
            case STRING:
                return  "String";
            case BOOL:
                return  "Boolean";
            case INT:
                return  "Integer";
            case FLOAT:
                return  "Float";
            default:
                throw new IllegalArgumentException();
        }
    }

    // TODO: passing viewTupleTypeParameters makes this class tightly coupled with OrToolsSolver.
    static String forExpr(final Expr expr, final Map<String, String> viewTupleTypeParameters) {
        final InferType visitor = new InferType(viewTupleTypeParameters);
        final String result =
            Objects.requireNonNull(visitor.visit(expr), "Result of type inference for expr was null: " + expr);
        assert !result.isEmpty();
        return result;
    }
}