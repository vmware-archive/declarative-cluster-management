/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.ComprehensionRewriter;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.VoidType;

import java.util.Objects;

/**
 * Minizinc has no "count()" function. We rewrite all instances of
 * count([i | qualifiers..]) to sum([1 | qualifiers...]).
 */
class RewriteCountFunction {

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final CountRewriter rewriter = new CountRewriter();
        final MonoidComprehension newCompr =
                (MonoidComprehension) Objects.requireNonNull(rewriter.visit(comprehension));
        return newCompr;
    }

    static class CountRewriter extends ComprehensionRewriter {
        @Override
        protected Expr visitMonoidFunction(final MonoidFunction function, final VoidType context) {
            if (function.getFunction().equals(MonoidFunction.Function.COUNT)) {
                if (!(function.getArgument().get(0) instanceof ColumnIdentifier)) {
                    throw new IllegalStateException("RewriteCountFunction is only safe to use on column identifiers");
                }
                final MonoidFunction newFunction = new MonoidFunction(MonoidFunction.Function.SUM,
                                                                      new MonoidLiteral<>(1L, Long.class));
                function.getAlias().ifPresent(newFunction::setAlias);
                return newFunction;
            }
            return super.visitMonoidFunction(function, context);
        }
    }
}