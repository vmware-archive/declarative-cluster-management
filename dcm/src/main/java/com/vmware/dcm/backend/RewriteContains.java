/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.vmware.dcm.compiler.monoid.MonoidFunction;
import com.vmware.dcm.compiler.monoid.VoidType;
import com.vmware.dcm.compiler.monoid.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.monoid.ComprehensionRewriter;
import com.vmware.dcm.compiler.monoid.Expr;
import com.vmware.dcm.compiler.monoid.GroupByComprehension;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteContains extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteContains.class);

    public static MonoidComprehension apply(final MonoidComprehension comprehension) {
        LOG.trace("Invoking RewriteContains on {}", comprehension);
        final RewriteContains rewriter = new RewriteContains();
        final Expr result = rewriter.visit(comprehension);
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
