/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package org.dcm.backend;

import com.google.common.base.Preconditions;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.ComprehensionRewriter;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Rewrites an expression of the form not(A) by the equivalent negated expression. This involves
 * flipping operators of the corresponding BinaryOperatorPredicates in A.
 */
class RewriteNot {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteNot.class);

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final RewriteNot.NotRewriter rewriter = new RewriteNot.NotRewriter();
        final Expr result = Objects.requireNonNull(rewriter.visit(comprehension, false));
        LOG.trace("Rewrote {} into {}", comprehension, result);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }

    private static class NotRewriter extends ComprehensionRewriter<Boolean> {
        @Override
        protected Expr visitMonoidFunction(final MonoidFunction node, @Nullable final Boolean flipOperators) {
            Preconditions.checkNotNull(flipOperators);
            if (node.getFunctionName().equalsIgnoreCase("not")) {
                if (flipOperators) {
                    return node.getArgument();
                } else {
                    return Objects.requireNonNull(visit(node.getArgument(), true));
                }
            }
            return super.visitMonoidFunction(node, false);
        }

        @Override
        protected Expr visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                    @Nullable final Boolean flipOperators) {
            if (flipOperators != null && flipOperators) {
                final BinaryOperatorPredicate ret;
                final Expr maybeFlippedLeft = Objects.requireNonNull(visit(node.getLeft(), flipOperators));
                final Expr maybeFlippedRight = Objects.requireNonNull(visit(node.getRight(), flipOperators));
                switch (node.getOperator()) {
                    case "==":
                        ret = new BinaryOperatorPredicate("!=", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case "!=":
                        ret = new BinaryOperatorPredicate("==", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case "/\\":
                        ret = new BinaryOperatorPredicate("\\/", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case "\\/":
                        ret = new BinaryOperatorPredicate("/\\", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case "<=":
                        ret = new BinaryOperatorPredicate(">", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case "<":
                        ret = new BinaryOperatorPredicate(">=", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case ">=":
                        ret = new BinaryOperatorPredicate("<", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    case ">":
                        ret = new BinaryOperatorPredicate("<=", maybeFlippedLeft, maybeFlippedRight);
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected operator " + node.getOperator());
                }
                return checkForAggregate(ret);
            }
            return super.visitBinaryOperatorPredicate(node, flipOperators);
        }

        private BinaryOperatorPredicate checkForAggregate(final BinaryOperatorPredicate node) {
            return node instanceof BinaryOperatorPredicateWithAggregate ?
                    new BinaryOperatorPredicateWithAggregate(node) : node;
        }
    }
}