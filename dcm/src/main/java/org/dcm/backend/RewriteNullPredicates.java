/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.ComprehensionRewriter;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.IsNotNullPredicate;
import org.dcm.compiler.monoid.IsNullPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Only used by the Minizinc backend to rewrite IsNull/IsNotNull(expr) predicates into expr =/!= 'null'
 */
public class RewriteNullPredicates {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteNullPredicates.class);

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final RewriteNullPredicates.Rewriter rewriter = new RewriteNullPredicates.Rewriter();
        final Expr result = Objects.requireNonNull(rewriter.visit(comprehension));
        LOG.trace("Rewrote {} into {}", comprehension, result);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }

    private static class Rewriter extends ComprehensionRewriter<Void> {
        @Nullable
        @Override
        protected Expr visitIsNullPredicate(final IsNullPredicate node, @Nullable final Void context) {
            return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.EQUAL, node,
                                               new MonoidLiteral<>("'null'", String.class));
        }

        @Nullable
        @Override
        protected Expr visitIsNotNullPredicate(final IsNotNullPredicate node, @Nullable final Void context) {
            return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.NOT_EQUAL, node,
                                               new MonoidLiteral<>("'null'", String.class));
        }
    }
}
