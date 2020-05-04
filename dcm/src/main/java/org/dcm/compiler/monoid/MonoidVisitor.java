/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;

public class MonoidVisitor<T, C> {

    @Nullable
    public T visit(final Expr expr, @Nullable final C context) {
        return expr.acceptVisitor(this, context);
    }

    @Nullable
    public T visit(final Expr expr) {
        return visit(expr, null);
    }

    @Nullable
    protected T visitHead(final Head node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitTableRowGenerator(final TableRowGenerator node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitMonoidComprehension(final MonoidComprehension node, @Nullable final C context) {
        node.getHead().acceptVisitor(this, context);
        for (final Qualifier qualifier: node.getQualifiers()) {
            qualifier.acceptVisitor(this, context);
        }
        return null;
    }

    @Nullable
    protected T visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, @Nullable final C context) {
        node.getLeft().acceptVisitor(this, context);
        node.getRight().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitGroupByComprehension(final GroupByComprehension node, @Nullable final C context) {
        node.getComprehension().acceptVisitor(this, context);
        node.getGroupByQualifier().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitGroupByQualifier(final GroupByQualifier node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitMonoidLiteral(final MonoidLiteral node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitMonoidFunction(final MonoidFunction node, @Nullable final C context) {
        for (final Expr expr: node.getArgument()) {
            expr.acceptVisitor(this, context);
        }
        return null;
    }

    @Nullable
    protected T visitUnaryOperator(final UnaryOperator node, @Nullable final C context) {
        node.getArgument().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitQualifier(final Qualifier node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitColumnIdentifier(final ColumnIdentifier node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitExistsPredicate(final ExistsPredicate node, @Nullable final C context) {
        node.getArgument().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitIsNullPredicate(final IsNullPredicate node, @Nullable final C context) {
        node.getArgument().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitIsNotNullPredicate(final IsNotNullPredicate node, @Nullable final C context) {
        node.getArgument().acceptVisitor(this, context);
        return null;
    }
}