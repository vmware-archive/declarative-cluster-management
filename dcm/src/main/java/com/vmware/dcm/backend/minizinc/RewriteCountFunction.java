/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.minizinc;

import com.vmware.dcm.compiler.monoid.MonoidFunction;
import com.vmware.dcm.compiler.monoid.VoidType;
import com.vmware.dcm.compiler.monoid.ColumnIdentifier;
import com.vmware.dcm.compiler.monoid.ComprehensionRewriter;
import com.vmware.dcm.compiler.monoid.Expr;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import com.vmware.dcm.compiler.monoid.MonoidLiteral;

/**
 * Minizinc has no "count()" function. We rewrite all instances of
 * count([i | qualifiers..]) to sum([1 | qualifiers...]).
 */
class RewriteCountFunction extends ComprehensionRewriter {

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

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final RewriteCountFunction rewriter = new RewriteCountFunction();
        return (MonoidComprehension) rewriter.visit(comprehension);
    }
}