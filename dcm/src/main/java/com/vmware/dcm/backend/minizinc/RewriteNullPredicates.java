/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.minizinc;

import com.vmware.dcm.compiler.monoid.IsNotNullPredicate;
import com.vmware.dcm.compiler.monoid.VoidType;
import com.vmware.dcm.compiler.monoid.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.monoid.ComprehensionRewriter;
import com.vmware.dcm.compiler.monoid.Expr;
import com.vmware.dcm.compiler.monoid.GroupByComprehension;
import com.vmware.dcm.compiler.monoid.IsNullPredicate;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import com.vmware.dcm.compiler.monoid.MonoidLiteral;
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
                                           new MonoidLiteral<>("'null'", String.class));
    }

    @Override
    protected Expr visitIsNotNullPredicate(final IsNotNullPredicate node, final VoidType context) {
        return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.NOT_EQUAL, node,
                                           new MonoidLiteral<>("'null'", String.class));
    }

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final RewriteNullPredicates rewriter = new RewriteNullPredicates();
        final Expr result = rewriter.visit(comprehension);
        LOG.trace("Rewrote {} into {}", comprehension, result);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }
}
