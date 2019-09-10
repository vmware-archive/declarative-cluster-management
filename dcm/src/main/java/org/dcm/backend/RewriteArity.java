/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;


import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.ComprehensionRewriter;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.Head;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.Qualifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Rewrites expressions that have uncertain arity (due to the use of option types) to
 * fixed arity expressions. This is done for sums by converting expressions of the form
 * sum([col | predicate-based-on-var...]) into expressions of the form
 * sum([col * (predicate-based-on-var) | non-var-q]).
 */
class RewriteArity {
    private static final Logger LOG = LoggerFactory.getLogger(RewriteArity.class);

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final ArityRewriter rewriter = new ArityRewriter();
        final Expr result = Objects.requireNonNull(rewriter.visit(comprehension));
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }

    private static class ArityRewriter extends ComprehensionRewriter<Void> {
        @Override
        protected Expr visitMonoidComprehension(final MonoidComprehension node, final Void context) {
            return rewriteComprehension(node);
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
                for (final Qualifier qualifier: varQualifiers) {
                    LOG.debug("--- " + qualifier);
                }
                return input;
            }
            final FunctionRewriter functionRewriter = new FunctionRewriter();
            final MonoidComprehension comprehensionWithoutVarQualifiers =
                    new MonoidComprehension(input.getHead(), nonVarQualifiers);
            final MonoidComprehension result =
                    (MonoidComprehension) functionRewriter.visit(comprehensionWithoutVarQualifiers,
                                                                 varQualifiers.get(0));
            if (functionRewriter.didRewrite) {
                LOG.debug("Rewrote: {} into {}", input, Objects.requireNonNull(result));
                return result;
            }
            else {
                LOG.debug("Did not rewrite: {}", input);
                return input;
            }
        }
    }

    /**
     * Rewrites sum/count functions such that the argument of the function is multiplied by a qualifier,
     * which is expected to be a predicate on a controllable column.
     */
    private static class FunctionRewriter extends ComprehensionRewriter<Qualifier> {
        private boolean didRewrite = false;
        private boolean isDepthPastOne = false;

        @Override
        protected Expr visitMonoidComprehension(final MonoidComprehension node, @Nullable final Qualifier context) {
            if (!isDepthPastOne) {
                isDepthPastOne = true;
                final List<Qualifier> qualifiers = node.getQualifiers()
                        .stream()
                        .map(q -> (Qualifier) super.visit(q, context))
                        .collect(Collectors.toList());
                final Head newHead = (Head) super.visit(Objects.requireNonNull(node.getHead()), context);
                return new MonoidComprehension(Objects.requireNonNull(newHead), qualifiers);
            }
            return node;
        }

        @Override
        protected Expr visitMonoidFunction(final MonoidFunction node, @Nullable final Qualifier qualifier) {
            assert qualifier != null;
            if (node.getFunctionName().equalsIgnoreCase("sum") ||
                node.getFunctionName().equalsIgnoreCase("count")) {
                final Expr oldSumArg = node.getFunctionName().equalsIgnoreCase("count")
                                        ? new MonoidLiteral<>(1, Integer.class) : node.getArgument();
                final BinaryOperatorPredicateWithAggregate newArgument
                        = new BinaryOperatorPredicateWithAggregate("*", oldSumArg, qualifier);
                didRewrite = true;
                if (node.getAlias().isPresent()) {
                    return new MonoidFunction(node.getFunctionName(), newArgument, node.getAlias().get());
                } else {
                    return new MonoidFunction(node.getFunctionName(), newArgument);
                }
            }
            if (node.getFunctionName().equalsIgnoreCase("-")) {
                final Expr ret = this.visit(node.getArgument(), qualifier);
                return new MonoidFunction("-", Objects.requireNonNull(ret));
            }
            return node;
        }
    }
}