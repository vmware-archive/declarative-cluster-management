/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

public class MonoidVisitor<T, C> {

    public T visit(final Expr expr, final C context) {
        return expr.acceptVisitor(this, context);
    }

    protected T visitHead(final Head node, final C context) {
        return defaultReturn();
    }

    protected T visitTableRowGenerator(final TableRowGenerator node, final C context) {
        return defaultReturn();
    }

    protected T visitCheckExpression(final CheckQualifier node, final C context) {
        node.getExpr().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T visitMonoidComprehension(final MonoidComprehension node, final C context) {
        node.getHead().acceptVisitor(this, context);
        for (final Qualifier qualifier: node.getQualifiers()) {
            qualifier.acceptVisitor(this, context);
        }
        return defaultReturn();
    }

    protected T visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final C context) {
        node.getLeft().acceptVisitor(this, context);
        node.getRight().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T visitGroupByComprehension(final GroupByComprehension node, final C context) {
        node.getComprehension().acceptVisitor(this, context);
        node.getGroupByQualifier().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T visitGroupByQualifier(final GroupByQualifier node, final C context) {
        return defaultReturn();
    }

    protected T visitMonoidLiteral(final MonoidLiteral node, final C context) {
        return defaultReturn();
    }

    protected T visitMonoidFunction(final MonoidFunction node, final C context) {
        for (final Expr expr: node.getArgument()) {
            expr.acceptVisitor(this, context);
        }
        return defaultReturn();
    }

    protected T visitUnaryOperator(final UnaryOperator node, final C context) {
        node.getArgument().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T visitQualifier(final Qualifier node, final C context) {
        return defaultReturn();
    }

    protected T visitColumnIdentifier(final ColumnIdentifier node, final C context) {
        return defaultReturn();
    }

    protected T visitExistsPredicate(final ExistsPredicate node, final C context) {
        node.getArgument().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T visitIsNullPredicate(final IsNullPredicate node, final C context) {
        node.getArgument().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T visitIsNotNullPredicate(final IsNotNullPredicate node, final C context) {
        node.getArgument().acceptVisitor(this, context);
        return defaultReturn();
    }

    protected T defaultReturn() {
        throw new UnsupportedOperationException();
    }
}