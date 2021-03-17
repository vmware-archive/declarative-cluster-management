/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.minizinc;

import com.vmware.dcm.compiler.ir.IsNotNullPredicate;
import com.vmware.dcm.compiler.ir.VoidType;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ComprehensionRewriter;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.IsNullPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Only used by the Minizinc backend to rewrite IsNull/IsNotNull(expr) predicates into expr =/!= 'null'
 */
public class RewriteNullPredicates extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteNullPredicates.class);

    @Override
    protected Expr visitIsNullPredicate(final IsNullPredicate node, final VoidType context) {
        return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.EQUAL, node,
                                           new Literal<>("'null'", String.class));
    }

    @Override
    protected Expr visitIsNotNullPredicate(final IsNotNullPredicate node, final VoidType context) {
        return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.NOT_EQUAL, node,
                                           new Literal<>("'null'", String.class));
    }

    static ListComprehension apply(final ListComprehension comprehension) {
        final RewriteNullPredicates rewriter = new RewriteNullPredicates();
        final Expr result = rewriter.visit(comprehension);
        LOG.trace("Rewrote {} into {}", comprehension, result);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (ListComprehension) result;
    }
}
