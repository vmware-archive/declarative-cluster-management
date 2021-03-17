/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

import java.util.List;
import java.util.stream.Collectors;

public class ComprehensionRewriter extends IRVisitor<Expr, VoidType> {

    public Expr visit(final Expr expr) {
        return super.visit(expr, VoidType.getAbsent());
    }

    @Override
    protected Expr visitHead(final Head node, final VoidType context) {
        final List<Expr> selectItems = node.getSelectExprs().stream().map(expr -> this.visit(expr, context))
                                           .collect(Collectors.toList());
        return new Head(selectItems);
    }

    @Override
    protected Expr visitCheckExpression(final CheckQualifier node, final VoidType context) {
        final Expr rewrittenExpr = this.visit(node.getExpr(), context);
        return new CheckQualifier(rewrittenExpr);
    }

    @Override
    protected Expr visitTableRowGenerator(final TableRowGenerator node, final VoidType context) {
        return new TableRowGenerator(node.getTable());
    }

    @Override
    protected Expr visitListComprehension(final ListComprehension node, final VoidType context) {
        final Head newHead = (Head) this.visitHead(node.getHead(), context);
        final List<Qualifier> newQualifiers = node.getQualifiers().stream()
                                                  .map(q -> (Qualifier) this.visit(q, context))
                                                  .collect(Collectors.toList());
        return new ListComprehension(newHead, newQualifiers);
    }

    @Override
    protected Expr visitUnaryOperator(final UnaryOperator node, final VoidType context) {
        final Expr argument = this.visit(node.getArgument(), context);
        return new UnaryOperator(node.getOperator(), argument);
    }

    @Override
    protected Expr visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                final VoidType context) {
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
    protected Expr visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
        final ListComprehension comprehension =
                (ListComprehension) this.visitListComprehension(node.getComprehension(), context);
        final GroupByQualifier qualifier =
                (GroupByQualifier) this.visitGroupByQualifier(node.getGroupByQualifier(), context);
        return new GroupByComprehension(comprehension, qualifier);
    }

    @Override
    protected Expr visitGroupByQualifier(final GroupByQualifier node, final VoidType context) {
        final List<Expr> maybeRewrittenExprs = node.getGroupByExprs()
                .stream().map(expr -> this.visit(expr, context))
                         .collect(Collectors.toList());
        return new GroupByQualifier(maybeRewrittenExprs);
    }

    @Override
    protected Expr visitLiteral(final Literal node, final VoidType context) {
        return node;
    }

    @Override
    protected Expr visitFunctionCall(final FunctionCall node, final VoidType context) {
        final List<Expr> arguments = node.getArgument().stream().map(this::visit)
                                         .collect(Collectors.toList());
        final FunctionCall function =  new FunctionCall(node.getFunction(), arguments);
        node.getAlias().ifPresent(function::setAlias);
        return function;
    }

    @Override
    protected Expr visitQualifier(final Qualifier node, final VoidType context) {
        return node;
    }

    @Override
    protected Expr visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        return node;
    }

    @Override
    protected Expr visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
        final Expr argument = this.visit(node.getArgument());
        return new ExistsPredicate(argument);
    }

    @Override
    protected Expr visitIsNullPredicate(final IsNullPredicate node, final VoidType context) {
        final Expr argument = this.visit(node.getArgument(), context);
        return new IsNullPredicate(argument);
    }

    @Override
    protected Expr visitIsNotNullPredicate(final IsNotNullPredicate node, final VoidType context) {
        final Expr argument = this.visit(node.getArgument(), context);
        return new IsNotNullPredicate(argument);
    }
}
