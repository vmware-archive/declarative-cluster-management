/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import com.google.common.base.Preconditions;
import com.vrg.IRColumn;
import com.vrg.compiler.monoid.BinaryOperatorPredicate;
import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.MonoidFunction;
import com.vrg.compiler.monoid.MonoidVisitor;

import javax.annotation.Nullable;

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
    protected String visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Void context) {
        return typeStringFromColumn(node);
    }

    @Nullable
    @Override
    protected String visitMonoidFunction(final MonoidFunction node, @Nullable final Void context) {
        return visit(node.getArgument(), context);
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
}