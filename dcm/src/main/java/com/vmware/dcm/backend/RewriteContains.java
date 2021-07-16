/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.VoidType;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ComprehensionRewriter;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.ListComprehension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites the CONTAINS(a, b) function call into the binary operation (A CONTAINS B)
 */
public class RewriteContains extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteContains.class);

    public static ListComprehension apply(final ListComprehension comprehension) {
        LOG.trace("Invoking RewriteContains on {}", comprehension);
        final RewriteContains rewriter = new RewriteContains();
        final Expr result = rewriter.visit(comprehension);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (ListComprehension) result;
    }

    @Override
    protected Expr visitFunctionCall(final FunctionCall node, final VoidType context) {
        if (node.getFunction().equals(FunctionCall.Function.CONTAINS)) {
            return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.CONTAINS,
                    node.getArgument().get(0), node.getArgument().get(1));
        }
        return super.visitFunctionCall(node, context);
    }
}
