/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.monoid;

public class BinaryOperatorPredicate extends Qualifier {
    private final Operator operator;
    private final Expr left;
    private final Expr right;

    public BinaryOperatorPredicate(final Operator operator, final Expr left, final Expr right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        return left + " " + operator + " " + right;
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, final C context) {
        return visitor.visitBinaryOperatorPredicate(this, context);
    }

    public Expr getLeft() {
        return left;
    }

    public Operator getOperator() {
        return operator;
    }

    public Expr getRight() {
        return right;
    }

    public enum Operator {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULUS,
        EQUAL,
        NOT_EQUAL,
        AND,
        OR,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        IN,
        CONTAINS
    }
}
