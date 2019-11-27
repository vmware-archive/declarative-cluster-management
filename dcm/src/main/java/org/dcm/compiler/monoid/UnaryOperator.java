/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;

public class UnaryOperator extends Expr {
    private final Operator operator;
    private final Expr argument;

    public UnaryOperator(final Operator operator, final Expr argument) {
        this.operator = operator;
        this.argument = argument;
    }

    public UnaryOperator(final Operator operator, final Expr argument, final String alias) {
        this.operator = operator;
        this.argument = argument;
        setAlias(alias);
    }

    public Expr getArgument() {
        return argument;
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        return "UnaryOperator{" +
                "operator='" + operator + '\'' +
                ", argument=" + argument +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitUnaryOperator(this, context);
    }

    public enum Operator {
        NOT,
        PLUS,
        MINUS
    }
}
