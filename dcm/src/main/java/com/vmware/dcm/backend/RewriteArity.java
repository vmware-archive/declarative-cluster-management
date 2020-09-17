/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;


import com.google.common.base.Preconditions;
import com.vmware.dcm.compiler.monoid.MonoidFunction;
import com.vmware.dcm.compiler.monoid.Qualifier;
import com.vmware.dcm.compiler.monoid.VoidType;
import com.vmware.dcm.compiler.monoid.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import com.vmware.dcm.compiler.monoid.ComprehensionRewriter;
import com.vmware.dcm.compiler.monoid.ExistsPredicate;
import com.vmware.dcm.compiler.monoid.Expr;
import com.vmware.dcm.compiler.monoid.GroupByComprehension;
import com.vmware.dcm.compiler.monoid.Head;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import com.vmware.dcm.compiler.monoid.MonoidLiteral;
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
class RewriteArity extends ComprehensionRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteArity.class);

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final RewriteArity rewriter = new RewriteArity();
        final Expr result = rewriter.visit(comprehension);
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }

    @Override
    protected Expr visitMonoidComprehension(final MonoidComprehension node, final VoidType context) {
        // First, rewrite qualifiers. Then, rewrite head
        final List<Qualifier> qualifiers = node.getQualifiers()
                .stream().map(q -> (Qualifier) visit(q)).collect(Collectors.toList());
        final MonoidComprehension maybeRewrittenComprehension = new MonoidComprehension(node.getHead(), qualifiers);
        return rewriteComprehension(maybeRewrittenComprehension);
    }

    @Override
    protected Expr visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
        Preconditions.checkArgument(node.getArgument() instanceof MonoidComprehension);
        final MonoidComprehension argument = rewriteExistsArgument((MonoidComprehension) node.getArgument());
        return new ExistsPredicate(argument);
    }

    /**
     * First, extract the var and non-var qualifiers in the comprehension. If we only have a single
     * var qualifier, then check if we can rewrite the comprehension. Rewrites only happen for
     * sum/count expressions.
     */
    private MonoidComprehension rewriteComprehension(final MonoidComprehension input) {
        LOG.debug("Attempting to rewrite: {}", input);

        // Extract var and non-var qualifiers
        final List<GetVarQualifiers.QualifiersList> collect = input.getQualifiers().stream()
                .map(GetVarQualifiers::apply)
                .collect(Collectors.toList());
        final List<Qualifier> varQualifiers = collect.stream().flatMap(ql -> ql.getVarQualifiers().stream())
                .collect(Collectors.toList());
        final List<Qualifier> nonVarQualifiers = collect.stream().flatMap(ql -> ql.getNonVarQualifiers().stream())
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
    private MonoidComprehension rewriteExistsArgument(final MonoidComprehension input) {
        LOG.debug("Attempting to rewrite: {}", input);

        final List<Qualifier> qualifiers = input.getQualifiers()
                .stream().map(q -> (Qualifier) visit(q)).collect(Collectors.toList());

        // Extract var and non-var qualifiers
        final List<GetVarQualifiers.QualifiersList> collect = qualifiers.stream()
                .map(GetVarQualifiers::apply)
                .collect(Collectors.toList());
        final List<Qualifier> varQualifiers = collect.stream().flatMap(ql -> ql.getVarQualifiers().stream())
                .collect(Collectors.toList());
        final List<Qualifier> nonVarQualifiers = collect.stream().flatMap(ql -> ql.getNonVarQualifiers().stream())
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
        final MonoidComprehension rewrittenComprehension =
                new MonoidComprehension(newHead, nonVarQualifiers);
        return rewrittenComprehension;
    }

    private static MonoidComprehension maybeRewriteFunctions(final MonoidComprehension input,
                                                             final List<Qualifier> varQualifiers,
                                                             final List<Qualifier> nonVarQualifiers) {
        final FunctionRewriter functionRewriter = new FunctionRewriter(varQualifiers.get(0));
        final MonoidComprehension comprehensionWithoutVarQualifiers =
                new MonoidComprehension(input.getHead(), nonVarQualifiers);
        final MonoidComprehension result =
                (MonoidComprehension) functionRewriter.visit(comprehensionWithoutVarQualifiers);
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
        protected Expr visitMonoidComprehension(final MonoidComprehension node, final VoidType context) {
            if (!isDepthPastOne) {
                isDepthPastOne = true;
                final List<Qualifier> qualifiers = node.getQualifiers()
                        .stream()
                        .map(q -> (Qualifier) super.visit(q, context))
                        .collect(Collectors.toList());

                final Head newHead = (Head) super.visit(node.getHead(), context);
                return new MonoidComprehension(newHead, qualifiers);
            }
            return node;
        }

        @Override
        protected Expr visitMonoidFunction(final MonoidFunction node, final VoidType context) {
            if (node.getFunction().equals(MonoidFunction.Function.SUM) ||
                    node.getFunction().equals(MonoidFunction.Function.COUNT)) {
                final Expr oldSumArg = node.getFunction().equals(MonoidFunction.Function.COUNT)
                        ? new MonoidLiteral<>(1, Integer.class) : node.getArgument().get(0);
                final BinaryOperatorPredicateWithAggregate newArgument
                        = new BinaryOperatorPredicateWithAggregate(BinaryOperatorPredicate.Operator.MULTIPLY,
                                                                   oldSumArg, qualifier);
                didRewrite = true;
                if (node.getAlias().isPresent()) {
                    return new MonoidFunction(node.getFunction(), newArgument, node.getAlias().get());
                } else {
                    return new MonoidFunction(node.getFunction(), newArgument);
                }
            }
            return node;
        }
    }
}