/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

import java.util.List;
import java.util.Optional;

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

    public FunctionCall(final Function function, final List<Expr> argument, final Optional<String> alias) {
        this.function = function;
        this.argument = argument;
        alias.ifPresent(this::setAlias);
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
        ANY,
        ALL,
        ALL_DIFFERENT,
        ALL_EQUAL,
        INCREASING,
        CAPACITY_CONSTRAINT,
        CONTAINS,
        SCALAR_PRODUCT
    }
}