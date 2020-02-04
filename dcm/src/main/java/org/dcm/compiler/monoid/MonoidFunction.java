/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;
import java.util.List;

public class MonoidFunction extends Expr {
    private final Function function;
    private final List<Expr> argument;

    public MonoidFunction(final Function function, final Expr argument) {
        this.function = function;
        this.argument = List.of(argument);
    }

    public MonoidFunction(final Function function, final List<Expr> argument) {
        this.function = function;
        this.argument = argument;
    }

    public MonoidFunction(final Function function, final Expr argument, final String alias) {
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
        return "MonoidFunction{" +
                "functionName='" + function + '\'' +
                ", argument=" + argument +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitMonoidFunction(this, context);
    }

    public enum Function {
        COUNT,
        SUM,
        MIN,
        MAX,
        ALL_DIFFERENT,
        ALL_EQUAL,
        INCREASING,
        CAPACITY_CONSTRAINT
    }
}