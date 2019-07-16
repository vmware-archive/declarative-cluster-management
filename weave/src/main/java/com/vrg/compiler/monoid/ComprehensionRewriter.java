package com.vrg.compiler.monoid;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ComprehensionRewriter extends MonoidVisitor<Expr, ComprehensionRewriter.ExprRewriter> {
    @Override
    public Expr visit(final Expr expr, @Nullable final ExprRewriter context) {
        return Objects.requireNonNull(context).rewrite(expr);
    }

    @Override
    public Expr visit(final Expr expr) {
        throw new UnsupportedOperationException("Rewriter must be invoked with a rewriter object");
    }

    @Override
    protected Expr visitHead(final Head node, @Nullable final ExprRewriter context) {
        final List<Expr> selectItems = node.getSelectExprs().stream().map(context::rewrite)
                                           .collect(Collectors.toList());
        return new Head(selectItems);
    }

    @Override
    protected Expr visitTableRowGenerator(final TableRowGenerator node, @Nullable final ExprRewriter context) {
        return context.rewrite(node);
    }

    @Override
    protected Expr visitMonoidComprehension(final MonoidComprehension node, @Nullable final ExprRewriter context) {
        final Head newHead = context.rewrite(node.getHead());
        final List<Qualifier> newQualifiers = node.getQualifiers().stream().map(context::rewrite)
                                                  .collect(Collectors.toList());
        return new MonoidComprehension(newHead, newQualifiers);
    }

    @Override
    protected Expr visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                @Nullable final ExprRewriter context) {
        final Expr left = context.rewrite(node.getLeft());
        final Expr right = context.rewrite(node.getRight());
        return new BinaryOperatorPredicate(node.getOperator(), left, right);
    }

    @Override
    protected Expr visitGroupByComprehension(final GroupByComprehension node, @Nullable final ExprRewriter context) {
        final MonoidComprehension comprehension = context.rewrite(node.getComprehension());
        final GroupByQualifier qualifier = context.rewrite(node.getGroupByQualifier());
        return new GroupByComprehension(comprehension, qualifier);
    }

    @Override
    protected Expr visitGroupByQualifier(final GroupByQualifier node, @Nullable final ExprRewriter context) {
        final List<ColumnIdentifier> columnIdentifiers = node.getColumnIdentifiers()
                .stream().map(context::rewrite).collect(Collectors.toList());
        return new GroupByQualifier(columnIdentifiers);
    }

    @Override
    protected Expr visitMonoidLiteral(final MonoidLiteral node, @Nullable final ExprRewriter context) {
        return context.rewrite(node);
    }

    @Override
    protected Expr visitMonoidFunction(final MonoidFunction node, @Nullable final ExprRewriter context) {
        final Expr arguments = context.rewrite(node.getArgument());
        return new MonoidFunction(node.getFunctionName(), arguments);
    }

    @Override
    protected Expr visitQualifier(final Qualifier node, @Nullable final ExprRewriter context) {
        return context.rewrite(node);
    }

    @Override
    protected Expr visitColumnIdentifier(final ColumnIdentifier node, @Nullable final ExprRewriter context) {
        return context.rewrite(node);
    }

    @Override
    protected Expr visitExistsPredicate(final ExistsPredicate node, @Nullable final ExprRewriter context) {
        final Expr argument = context.rewrite(node.getArgument());
        return new ExistsPredicate(argument);
    }

    public static class ExprRewriter {
        public <E extends Expr> E rewrite(final E expr) {
            return expr;
        }
    }
}
