/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;

public class MonoidLiteral<T> extends Expr {
    private final T value;

    public MonoidLiteral(final T value, final Class<T> type) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "MonoidLiteral{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    <T1, C> T1 acceptVisitor(final MonoidVisitor<T1, C> visitor, @Nullable final C context) {
        return visitor.visitMonoidLiteral(this, context);
    }
}
