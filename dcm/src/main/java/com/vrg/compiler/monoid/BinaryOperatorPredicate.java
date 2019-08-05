/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.compiler.monoid;

import javax.annotation.Nullable;

public class BinaryOperatorPredicate extends Qualifier {
    private final String operator;
    private final Expr left;
    private final Expr right;

    public BinaryOperatorPredicate(final String operator, final Expr left, final Expr right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        return left + " " + operator + " " + right;
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitBinaryOperatorPredicate(this, context);
    }

    public Expr getLeft() {
        return left;
    }

    public String getOperator() {
        return operator;
    }

    public Expr getRight() {
        return right;
    }
}
