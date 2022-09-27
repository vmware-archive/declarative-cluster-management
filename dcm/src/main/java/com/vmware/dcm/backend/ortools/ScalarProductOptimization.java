/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ComprehensionRewriter;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.VoidType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * A sum of an expression of the form (X * Y), where X is a variable and Y is a constant, is best
 * expressed as a scalar product which takes a list of variables, and a corresponding list of coefficients.
 * This is common in bin-packing problems where a list of variables corresponding to whether a task is
 * assigned to a bin, is weighted by the demands of each task, and the resulting weighted sum of variables
 * is used in capacity constraints.
 *
 * This optimization saves the CP-SAT solver a lot of effort in the pre-solving and solving phases,
 * leading to significant performance improvements.
 *
 * If we cannot perform this optimization, we revert to computing a sum just like any other function.
 *
 */
class ScalarProductOptimization extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(ScalarProductOptimization.class);
    private final TupleMetadata metadata;

    ScalarProductOptimization(final TupleMetadata metadata) {
        this.metadata = metadata;
    }

    static ListComprehension apply(final ListComprehension comprehension, final TupleMetadata metadata) {
        LOG.trace("Invoking ScalarProductOptimization on {}", comprehension);
        final ScalarProductOptimization rewriter = new ScalarProductOptimization(metadata);
        final Expr result = rewriter.visit(comprehension);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (ListComprehension) result;
    }

    @Override
    protected Expr visitFunctionCall(final FunctionCall node, final VoidType context) {
        if ((node.getFunction() == FunctionCall.Function.SUM
             || node.getFunction() == FunctionCall.Function.COUNT)) {
            final Expr argument = node.getArgument().get(0);
            if (argument instanceof BinaryOperatorPredicate) {
                final BinaryOperatorPredicate operation = ((BinaryOperatorPredicate) argument);
                final BinaryOperatorPredicate.Operator op = operation.getOperator();
                final Expr left = operation.getLeft();
                final Expr right = operation.getRight();
                final JavaType leftType = metadata.inferType(left);
                final JavaType rightType = metadata.inferType(right);
                // TODO: The multiply may not necessarily be the top level operation.
                if (op.equals(BinaryOperatorPredicate.Operator.MULTIPLY)) {
                    if (JavaType.isVar(leftType) && !JavaType.isVar(rightType)) {
                        return new FunctionCall(FunctionCall.Function.SCALAR_PRODUCT, List.of(left, right),
                                                node.getAlias());
                    }
                    if (JavaType.isVar(rightType) && !JavaType.isVar(leftType)) {
                        return new FunctionCall(FunctionCall.Function.SCALAR_PRODUCT, List.of(right, left),
                                                node.getAlias());
                    }
                }
            }
        }
        return super.visitFunctionCall(node, context);
    }
}
