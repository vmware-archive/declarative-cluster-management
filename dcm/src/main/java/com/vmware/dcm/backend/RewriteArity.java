/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;


import com.google.common.base.Preconditions;
import com.vmware.dcm.compiler.UsesControllableFields;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicateWithAggregate;
import com.vmware.dcm.compiler.ir.ComprehensionRewriter;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.Head;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.Qualifier;
import com.vmware.dcm.compiler.ir.VoidType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rewrites expressions that have uncertain arity (due to the use of option types) to
 * fixed arity expressions. This is done for sums by converting expressions of the form
 * sum([col | predicate-based-on-var...]) into expressions of the form
 * sum([col * (predicate-based-on-var) | non-var-q]).
 */
public class RewriteArity extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteArity.class);

    public static ListComprehension apply(final ListComprehension comprehension) {
        final RewriteArity rewriter = new RewriteArity();
        final Expr result = rewriter.visit(comprehension);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (ListComprehension) result;
    }

    @Override
    protected Expr visitListComprehension(final ListComprehension node, final VoidType context) {
        // First, rewrite qualifiers. Then, rewrite head
        final List<Qualifier> qualifiers = node.getQualifiers()
                .stream().map(q -> (Qualifier) visit(q)).collect(Collectors.toList());
        final ListComprehension maybeRewrittenComprehension = new ListComprehension(node.getHead(), qualifiers);
        return rewriteComprehension(maybeRewrittenComprehension);
    }

    @Override
    protected Expr visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
        Preconditions.checkArgument(node.getArgument() instanceof ListComprehension);
        final ListComprehension argument = rewriteExistsArgument((ListComprehension) node.getArgument());
        return new ExistsPredicate(argument);
    }

    /**
     * First, extract the var and non-var qualifiers in the comprehension. If we only have a single
     * var qualifier, then check if we can rewrite the comprehension. Rewrites only happen for
     * sum/count expressions.
     */
    private ListComprehension rewriteComprehension(final ListComprehension input) {
        LOG.debug("Attempting to rewrite: {}", input);
        final List<Qualifier> varQualifiers = input.getQualifiers().stream()
                .filter(UsesControllableFields::apply)
                .collect(Collectors.toList());
        final List<Qualifier> nonVarQualifiers = input.getQualifiers().stream()
                .filter(e -> !UsesControllableFields.apply(e))
                .collect(Collectors.toList());
        if (varQualifiers.isEmpty()) {
            return input;
        }
        if (varQualifiers.size() != 1) {
            LOG.debug("Found multiple var qualifiers. Skipping arity rewrite.");
            for (final Qualifier qualifier : varQualifiers) {
                LOG.debug("--- " + qualifier);
            }
            return input;
        }
        return maybeRewriteFunctions(input, varQualifiers, nonVarQualifiers);
    }


    /**
     * First, extract the var and non-var qualifiers in the comprehension. If we only have a single
     * var qualifier, then ignore the head items and replace it with the var qualifier
     */
    private ListComprehension rewriteExistsArgument(final ListComprehension input) {
        LOG.debug("Attempting to rewrite: {}", input);

        final List<Qualifier> varQualifiers = input.getQualifiers().stream()
                .filter(UsesControllableFields::apply)
                .collect(Collectors.toList());
        final List<Qualifier> nonVarQualifiers = input.getQualifiers().stream()
                .filter(e -> !UsesControllableFields.apply(e))
                .collect(Collectors.toList());
        if (varQualifiers.isEmpty()) {
            return input;
        }
        if (varQualifiers.size() != 1) {
            LOG.debug("Found multiple var qualifiers. Skipping arity rewrite.");
            for (final Qualifier qualifier : varQualifiers) {
                LOG.debug("--- " + qualifier);
            }
            return input;
        }
        final Head newHead = new Head(Collections.singletonList(varQualifiers.get(0)));
        return new ListComprehension(newHead, nonVarQualifiers);
    }

    private static ListComprehension maybeRewriteFunctions(final ListComprehension input,
                                                           final List<Qualifier> varQualifiers,
                                                           final List<Qualifier> nonVarQualifiers) {
        final FunctionRewriter functionRewriter = new FunctionRewriter(varQualifiers.get(0));
        final ListComprehension comprehensionWithoutVarQualifiers =
                new ListComprehension(input.getHead(), nonVarQualifiers);
        final ListComprehension result =
                (ListComprehension) functionRewriter.visit(comprehensionWithoutVarQualifiers);
        if (functionRewriter.didRewrite) {
            LOG.info("Rewrote: {} into {}", input, result);
            return result;
        } else {
            LOG.debug("Did not rewrite: {}", input);
            return input;
        }
    }

    /**
     * Rewrites sum/count functions such that the argument of the function is multiplied by a qualifier,
     * which is expected to be a predicate on a controllable column.
     */
    private static class FunctionRewriter extends ComprehensionRewriter {
        private boolean didRewrite = false;
        private boolean isDepthPastOne = false;
        private final Qualifier qualifier;

        public FunctionRewriter(final Qualifier qualifier) {
            this.qualifier = qualifier;
        }

        @Override
        protected Expr visitListComprehension(final ListComprehension node, final VoidType context) {
            if (!isDepthPastOne) {
                isDepthPastOne = true;
                final List<Qualifier> qualifiers = node.getQualifiers()
                        .stream()
                        .map(q -> (Qualifier) super.visit(q, context))
                        .collect(Collectors.toList());

                final Head newHead = (Head) super.visit(node.getHead(), context);
                return new ListComprehension(newHead, qualifiers);
            }
            return node;
        }

        @Override
        protected Expr visitFunctionCall(final FunctionCall node, final VoidType context) {
            if (node.getFunction().equals(FunctionCall.Function.SUM) ||
                    node.getFunction().equals(FunctionCall.Function.COUNT)) {
                final Expr oldSumArg = node.getFunction().equals(FunctionCall.Function.COUNT)
                        ? new Literal<>(1L, Long.class) : node.getArgument().get(0);
                final BinaryOperatorPredicateWithAggregate newArgument
                        = new BinaryOperatorPredicateWithAggregate(BinaryOperatorPredicate.Operator.MULTIPLY,
                                                                   oldSumArg, qualifier);
                didRewrite = true;
                if (node.getAlias().isPresent()) {
                    return new FunctionCall(node.getFunction(), newArgument, node.getAlias().get());
                } else {
                    return new FunctionCall(node.getFunction(), newArgument);
                }
            }
            return node;
        }
    }
}