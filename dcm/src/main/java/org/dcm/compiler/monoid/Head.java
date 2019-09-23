/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;
import java.util.List;

public final class Head extends Expr {
    private final List<Expr> selectExprs;

    public Head(final List<Expr> selectExprs) {
        this.selectExprs = selectExprs;
    }

    @Override
    public String toString() {
        return "Head{" +
                "selectExprs=" + selectExprs +
                '}';
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitHead(this, context);
    }

    public List<Expr> getSelectExprs() {
        return selectExprs;
    }
}