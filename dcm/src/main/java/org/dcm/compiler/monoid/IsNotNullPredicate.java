/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;

public class IsNotNullPredicate extends Expr {
    private final Expr argument;

    public IsNotNullPredicate(final Expr argument) {
        this.argument = argument;
    }

    public Expr getArgument() {
        return argument;
    }

    @Override
    public String toString() {
        return "IsNotNull{" +
                "argument=" + argument +
                '}';
    }

    @Nullable
    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitIsNotNullPredicate(this, context);
    }
}