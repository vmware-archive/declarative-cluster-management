package com.vrg;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;

import java.util.Optional;

/**
 * Remove parts of an expression that involve controllable variables.
 */
class RemoveControllablePredicates extends DefaultTraversalVisitor<Optional<Expression>, Void> {

    @Override
    protected Optional<Expression> visitLogicalBinaryExpression(final LogicalBinaryExpression node,
                                                                final Void context) {
        final Optional<Expression> left = this.process(node.getLeft());
        final Optional<Expression> right = this.process(node.getRight());
        switch (node.getOperator()) {
            case AND:
                if (!left.isPresent() && !right.isPresent()) {
                    return Optional.empty();
                }
                else if (!left.isPresent()) {
                    return right;
                }
                else if (!right.isPresent()) {
                    return left;
                }
                else {
                    return Optional.of(node);
                }
            case OR:
                return (!left.isPresent() || !right.isPresent()) ? Optional.empty() : Optional.of(node);
            default:
                throw new RuntimeException("Should not happen");
        }
    }

    @Override
    protected Optional<Expression> visitComparisonExpression(final ComparisonExpression node, final Void context) {
        final Optional<Expression> left = this.process(node.getLeft());
        final Optional<Expression> right = this.process(node.getRight());
        return (!left.isPresent() || !right.isPresent()) ? Optional.empty() : Optional.of(node);
    }

    @Override
    protected Optional<Expression> visitDereferenceExpression(final DereferenceExpression node, final Void context) {
        return node.getField().getValue().startsWith("controllable__") ? Optional.empty() : Optional.of(node);
    }

    @Override
    protected Optional<Expression> visitIdentifier(final Identifier node, final Void context) {
        return node.getValue().startsWith("controllable__") ? Optional.empty() : Optional.of(node);
    }
}
