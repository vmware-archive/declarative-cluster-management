/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.minizinc;

import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.ComprehensionRewriter;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.VoidType;

/**
 * Rewrite instances of count([i | qualifiers..]) to sum([1 | qualifiers...]).
 */
public class RewriteCountFunction extends ComprehensionRewriter {

    @Override
    protected Expr visitFunctionCall(final FunctionCall function, final VoidType context) {
        if (function.getFunction().equals(FunctionCall.Function.COUNT)) {
            if (!(function.getArgument().get(0) instanceof ColumnIdentifier)) {
                throw new IllegalStateException("RewriteCountFunction is only safe to use on column identifiers");
            }
            final FunctionCall newFunction = new FunctionCall(FunctionCall.Function.SUM,
                                                                  new Literal<>(1L, Long.class));
            function.getAlias().ifPresent(newFunction::setAlias);
            return newFunction;
        }
        return super.visitFunctionCall(function, context);
    }

    public static ListComprehension apply(final ListComprehension comprehension) {
        final RewriteCountFunction rewriter = new RewriteCountFunction();
        return (ListComprehension) rewriter.visit(comprehension);
    }
}