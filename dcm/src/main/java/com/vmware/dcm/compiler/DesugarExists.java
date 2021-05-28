/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ComprehensionRewriter;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.VoidType;

import java.util.ArrayList;
import java.util.List;

/*
 * Expands an exists() predicate into "exists() = true"
 */
class DesugarExists {

    static ListComprehension apply(final ListComprehension view) {
        final DesugarExistsInner inner = new DesugarExistsInner();
        return (ListComprehension) inner.visit(view);
    }

    private static class DesugarExistsInner extends ComprehensionRewriter {
        private final List<Expr> stack = new ArrayList<>();

        @Override
        public Expr visit(final Expr expr, final VoidType context) {
            stack.add(expr);
            final Expr result = super.visit(expr, context);
            stack.remove(stack.size() - 1);
            return result;
        }

        @Override
        protected Expr visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
            // The node being visited is at the top of the stack. Peek one level behind it for the ancestor.
            final Expr ancestor = stack.get(stack.size() - 2);
            if (ancestor instanceof BinaryOperatorPredicate) {
                final BinaryOperatorPredicate.Operator op = ((BinaryOperatorPredicate) ancestor).getOperator();
                switch (op) {
                    case EQUAL:
                    case NOT_EQUAL:
                        return super.visitExistsPredicate(node, context);
                    default:
                        break;
                }
            }
            return new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.EQUAL, node,
                    new Literal<>(true, Boolean.class));
        }
    }
}
