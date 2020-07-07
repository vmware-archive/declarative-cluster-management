/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.ComprehensionRewriter;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.VoidType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class RewriteContains extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteContains.class);

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        LOG.trace("Invoking RewriteContains on {}", comprehension);
        final RewriteContains rewriter = new RewriteContains();
        final Expr result = Objects.requireNonNull(rewriter.visit(comprehension));
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }

    @Override
    protected Expr visitMonoidFunction(final MonoidFunction node, final VoidType context) {
        if (node.getFunction().equals(MonoidFunction.Function.CONTAINS)) {
            return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.CONTAINS,
                    node.getArgument().get(0), node.getArgument().get(1));
        }
        return super.visitMonoidFunction(node, context);
    }
}
