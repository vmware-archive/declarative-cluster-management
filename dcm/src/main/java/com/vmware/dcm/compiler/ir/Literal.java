/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

public class Literal<T> extends Expr {
    private final T value;

    public Literal(final T value, final Class<T> type) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Literal{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    <T1, C> T1 acceptVisitor(final IRVisitor<T1, C> visitor, final C context) {
        return visitor.visitLiteral(this, context);
    }
}
