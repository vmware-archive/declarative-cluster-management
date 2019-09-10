/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.google.common.base.Preconditions;
import org.dcm.IRColumn;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.MonoidVisitor;

import javax.annotation.Nullable;
import java.util.Objects;

class InferType extends MonoidVisitor<String, Void> {
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
            case "==":
            case "!=":
            case "/\\":
            case "\\/":
            case "<=":
            case "<":
            case ">=":
            case ">":
                return "Boolean";
            case "+":
            case "-":
            case "*":
            case "/":
                return "Integer";
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Nullable
    @Override
    protected String visitMonoidComprehension(final MonoidComprehension node, @Nullable final Void context) {
        return "SubQuery";
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

    private static String typeStringFromColumn(final ColumnIdentifier node) {
        if (node.getField().isControllable()) {
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

    static String forExpr(final Expr expr) {
        final InferType visitor = new InferType();
        final String result =
            Objects.requireNonNull(visitor.visit(expr), "Result of type inference for expr was null: " + expr);
        assert !result.isEmpty();
        return result;
    }
}