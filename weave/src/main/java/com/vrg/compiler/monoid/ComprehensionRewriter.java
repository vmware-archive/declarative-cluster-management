/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.compiler.monoid;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class ComprehensionRewriter<T> extends MonoidVisitor<Expr, T> {
    @Override
    protected Expr visitHead(final Head node, @Nullable final T context) {
        final List<Expr> selectItems = node.getSelectExprs().stream().map(expr -> this.visit(expr, context))
                                           .collect(Collectors.toList());
        return new Head(selectItems);
    }

    @Override
    protected Expr visitTableRowGenerator(final TableRowGenerator node, @Nullable final T context) {
        return new TableRowGenerator(node.getTable());
    }

    @Override
    protected Expr visitMonoidComprehension(final MonoidComprehension node, @Nullable final T context) {
        final Head newHead = (Head) this.visitHead(node.getHead(), context);
        final List<Qualifier> newQualifiers = node.getQualifiers().stream()
                                                  .map(q -> (Qualifier) this.visit(q, context))
                                                  .collect(Collectors.toList());
        return new MonoidComprehension(newHead, newQualifiers);
    }

    @Override
    protected Expr visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                @Nullable final T context) {
        final Expr left = this.visit(node.getLeft(), context);
        final Expr right = this.visit(node.getRight(), context);
        final BinaryOperatorPredicate predicate =
                node instanceof BinaryOperatorPredicateWithAggregate ?
                        new BinaryOperatorPredicateWithAggregate(node.getOperator(), left, right) :
                        new BinaryOperatorPredicate(node.getOperator(), left, right);
        node.getAlias().ifPresent(predicate::setAlias);
        return node instanceof JoinPredicate ? new JoinPredicate(predicate) : predicate;
    }

    @Override
    protected Expr visitGroupByComprehension(final GroupByComprehension node, @Nullable final T context) {
        final MonoidComprehension comprehension =
                (MonoidComprehension) this.visitMonoidComprehension(node.getComprehension(), context);
        final GroupByQualifier qualifier =
                (GroupByQualifier) this.visitGroupByQualifier(node.getGroupByQualifier(), context);
        return new GroupByComprehension(comprehension, qualifier);
    }

    @Override
    protected Expr visitGroupByQualifier(final GroupByQualifier node, @Nullable final T context) {
        final List<ColumnIdentifier> columnIdentifiers = node.getColumnIdentifiers()
                .stream().map(ci -> (ColumnIdentifier) this.visitColumnIdentifier(ci, context))
                         .collect(Collectors.toList());
        return new GroupByQualifier(columnIdentifiers);
    }

    @Override
    protected Expr visitMonoidLiteral(final MonoidLiteral node, @Nullable final T context) {
        return node;
    }

    @Override
    protected Expr visitMonoidFunction(final MonoidFunction node, @Nullable final T context) {
        final Expr arguments = this.visit(node.getArgument());
        final MonoidFunction function =  new MonoidFunction(node.getFunctionName(), arguments);
        node.getAlias().ifPresent(function::setAlias);
        return function;
    }

    @Override
    protected Expr visitQualifier(final Qualifier node, @Nullable final T context) {
        return node;
    }

    @Override
    protected Expr visitColumnIdentifier(final ColumnIdentifier node, @Nullable final T context) {
        return node;
    }

    @Override
    protected Expr visitExistsPredicate(final ExistsPredicate node, @Nullable final T context) {
        final Expr argument = this.visit(node.getArgument());
        return new ExistsPredicate(argument);
    }
}
