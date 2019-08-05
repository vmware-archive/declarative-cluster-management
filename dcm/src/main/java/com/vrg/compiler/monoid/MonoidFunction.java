/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.compiler.monoid;

import javax.annotation.Nullable;

public class MonoidFunction extends Expr {
    private final String functionName;
    private final Expr argument;

    public MonoidFunction(final String functionName, final Expr argument) {
        this.functionName = functionName;
        this.argument = argument;
    }

    public MonoidFunction(final String functionName, final Expr argument, final String alias) {
        this.functionName = functionName;
        this.argument = argument;
        setAlias(alias);
    }

    public Expr getArgument() {
        return argument;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public String toString() {
        return "MonoidFunction{" +
                "functionName='" + functionName + '\'' +
                ", argument=" + argument +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitMonoidFunction(this, context);
    }
}