/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

import java.util.List;

public class FunctionCall extends Expr {
    private final Function function;
    private final List<Expr> argument;

    public FunctionCall(final Function function, final Expr argument) {
        this.function = function;
        this.argument = List.of(argument);
    }

    public FunctionCall(final Function function, final List<Expr> argument) {
        this.function = function;
        this.argument = argument;
    }

    public FunctionCall(final Function function, final Expr argument, final String alias) {
        this.function = function;
        this.argument = List.of(argument);
        setAlias(alias);
    }

    public List<Expr> getArgument() {
        return argument;
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public String toString() {
        return "FunctionCall{" +
                "functionName='" + function + '\'' +
                ", argument=" + argument +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final IRVisitor<T, C> visitor, final C context) {
        return visitor.visitFunctionCall(this, context);
    }

    public enum Function {
        COUNT,
        SUM,
        MIN,
        MAX,
        ALL_DIFFERENT,
        ALL_EQUAL,
        INCREASING,
        CAPACITY_CONSTRAINT,
        CONTAINS
    }
}