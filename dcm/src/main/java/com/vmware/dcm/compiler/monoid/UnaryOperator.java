/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.monoid;

public class UnaryOperator extends Expr {
    private final Operator operator;
    private final Expr argument;

    public UnaryOperator(final Operator operator, final Expr argument) {
        this.operator = operator;
        this.argument = argument;
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
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, final C context) {
        return visitor.visitUnaryOperator(this, context);
    }

    public enum Operator {
        NOT,
        PLUS,
        MINUS
    }
}
