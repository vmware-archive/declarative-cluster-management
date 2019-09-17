/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.UsesControllableFields;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.ComprehensionRewriter;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.GroupByQualifier;
import org.dcm.compiler.monoid.Head;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidVisitor;
import org.dcm.compiler.monoid.Qualifier;
import org.dcm.compiler.monoid.TableRowGenerator;
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
            List<QualifiersList> collect = input.getQualifiers().stream()
                                                .map(q -> {
                                                    final GetVarQualifiers visitor = new GetVarQualifiers();
                                                    return visitor.visit(q, new QualifiersList());
                                                })
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
     * Separates qualifiers in an expression into vars and non-vars.
     */
    private static class GetVarQualifiers extends MonoidVisitor<QualifiersList, QualifiersList> {

        @Override
        protected QualifiersList visitGroupByQualifier(final GroupByQualifier node,
                                                       @Nullable final QualifiersList context) {
            assert context != null;
            return context.withNonVarQualifier(node);
        }

        @Override
        protected QualifiersList visitGroupByComprehension(final GroupByComprehension node,
                                                           @Nullable final QualifiersList context) {
            return context;
        }

        @Override
        protected QualifiersList visitMonoidComprehension(final MonoidComprehension node,
                                                          @Nullable final QualifiersList context) {
            return context;
        }

        @Override
        protected QualifiersList visitTableRowGenerator(final TableRowGenerator node,
                                                        @Nullable final QualifiersList context) {
            assert context != null;
            return context.withNonVarQualifier(node);
        }

        @Override
        protected QualifiersList visitMonoidFunction(final MonoidFunction node,
                                                     @Nullable final QualifiersList context) {
            return context;
        }

        @Override
        protected QualifiersList visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                              final QualifiersList context) {
            switch (node.getOperator()) {
                case "==":
                case "<":
                case ">":
                case "<=":
                case ">=":
                case "!=":
                case "in":
                case "\\/": {
                    // function expressions do not necessarily affect the arity of an outer expression.
                    // We err on the conservative side for now.
                    if ((isControllableField(node.getLeft()) || isControllableField(node.getRight()))
                         && (!(node.getLeft() instanceof MonoidFunction)
                             && !(node.getRight() instanceof MonoidFunction))) {
                        return context.withVarQualifier(node);
                    }
                    return context.withNonVarQualifier(node);
                }
                case "/\\": {
                    final QualifiersList left = Objects.requireNonNull(visit(node.getLeft(), context));
                    final QualifiersList right = Objects.requireNonNull(visit(node.getRight(), context));
                    return left.withQualifiersList(right);
                }
                default:
                    throw new RuntimeException("Missing case " + node.getOperator());
            }
        }

        private boolean isControllableField(final Expr expr) {
            final UsesControllableFields usesControllableFields = new UsesControllableFields();
            usesControllableFields.visit(expr);
            return usesControllableFields.usesControllableFields();
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
                final Expr oldSumArg = node.getArgument();
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