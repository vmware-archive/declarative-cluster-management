/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;

public class IsNullPredicate extends Expr {
    private final Expr argument;

    public IsNullPredicate(final Expr argument) {
        this.argument = argument;
    }

    public Expr getArgument() {
        return argument;
    }

    @Override
    public String toString() {
        return "IsNull{" +
                "argument=" + argument +
                '}';
    }

    @Nullable
    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitIsNullPredicate(this, context);
    }
}
