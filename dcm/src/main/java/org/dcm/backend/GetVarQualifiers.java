/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;


import org.dcm.compiler.UsesControllableFields;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.GroupByQualifier;
import org.dcm.compiler.monoid.IsNotNullPredicate;
import org.dcm.compiler.monoid.IsNullPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.MonoidVisitor;
import org.dcm.compiler.monoid.Qualifier;
import org.dcm.compiler.monoid.TableRowGenerator;
import org.dcm.compiler.monoid.UnaryOperator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Separates qualifiers in an expression into vars and non-vars.
 */
class GetVarQualifiers extends MonoidVisitor<GetVarQualifiers.QualifiersList, GetVarQualifiers.QualifiersList> {
    private final boolean skipAggregates;

    GetVarQualifiers(final boolean skipAggregates) {
        this.skipAggregates = skipAggregates;
    }

    static QualifiersList apply(final Expr expr, final boolean skipAggregates) {
        final GetVarQualifiers visitor = new GetVarQualifiers(skipAggregates);
        return Objects.requireNonNull(visitor.visit(expr));
    }

    static QualifiersList apply(final Expr expr) {
        return apply(expr, true);
    }

    @Nullable
    @Override
    public QualifiersList visit(final Expr expr) {
        return super.visit(expr, new GetVarQualifiers.QualifiersList());
    }

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

    @Nullable
    @Override
    protected QualifiersList visitUnaryOperator(final UnaryOperator node, @Nullable final QualifiersList context) {
        // TODO: revisit the sub-types of Qualifiers. Not every qualifier needs to be a binary operator.
        if (node.getOperator().equals(UnaryOperator.Operator.NOT)) {
            final BinaryOperatorPredicate rewritten =
                    new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.EQUAL, node.getArgument(),
                            new MonoidLiteral<>(false, Boolean.class));
            if (isControllableField(node.getArgument())) {
                return Objects.requireNonNull(context).withVarQualifier(rewritten);
            } else {
                return Objects.requireNonNull(context).withNonVarQualifier(rewritten);
            }
        }
        return context;
    }

    @Override
    protected QualifiersList visitMonoidFunction(final MonoidFunction node,
                                                 @Nullable final QualifiersList context) {
        return context;
    }

    @Nullable
    @Override
    protected QualifiersList visitIsNotNullPredicate(final IsNotNullPredicate node,
                                                     @Nullable final QualifiersList context) {
        return context;
    }

    @Nullable
    @Override
    protected QualifiersList visitIsNullPredicate(final IsNullPredicate node, @Nullable final QualifiersList context) {
        return context;
    }

    @Override
    protected QualifiersList visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                          final QualifiersList context) {
        switch (node.getOperator()) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IN:
            case OR: {
                // function expressions do not necessarily affect the arity of an outer expression.
                // We err on the conservative side for now.
                if ((isControllableField(node.getLeft()) || isControllableField(node.getRight()))
                        && involvesAggregateFunctions(node)) {
                    return context.withVarQualifier(checkForAggregate(node));
                }
                return context.withNonVarQualifier(checkForAggregate(node));
            }
            case AND: {
                final QualifiersList left = Objects.requireNonNull(visit(node.getLeft(), context));
                final QualifiersList right = Objects.requireNonNull(visit(node.getRight(), context));
                return left.withQualifiersList(right);
            }
            default:
                throw new RuntimeException("Missing case " + node.getOperator());
        }
    }

    private boolean involvesAggregateFunctions(final BinaryOperatorPredicate node) {
        if (skipAggregates) {
            return (!(node.getLeft() instanceof MonoidFunction) && !(node.getRight() instanceof MonoidFunction));
        }
        else {
            return true;
        }
    }

    private BinaryOperatorPredicate checkForAggregate(final BinaryOperatorPredicate node) {
        return node instanceof BinaryOperatorPredicateWithAggregate ?
                new BinaryOperatorPredicateWithAggregate(node) : node;
    }

    private boolean isControllableField(final Expr expr) {
        final UsesControllableFields usesControllableFields = new UsesControllableFields();
        usesControllableFields.visit(expr);
        return usesControllableFields.usesControllableFields();
    }


    /**
     * Container to propagate var and non-var qualifiers
     */
    static class QualifiersList {
        private final List<Qualifier> varQualifiers;
        private final List<Qualifier> nonVarQualifiers;

        QualifiersList() {
            this.varQualifiers = new ArrayList<>();
            this.nonVarQualifiers = new ArrayList<>();
        }

        private QualifiersList(final List<Qualifier> varQualifiers, final List<Qualifier> nonVarQualifiers) {
            this.varQualifiers = new ArrayList<>(varQualifiers);
            this.nonVarQualifiers = new ArrayList<>(nonVarQualifiers);
        }

        private QualifiersList withNonVarQualifier(final Qualifier qualifier) {
            final List<Qualifier> newNonVarQualifiers = new ArrayList<>(nonVarQualifiers);
            newNonVarQualifiers.add(qualifier);
            return new QualifiersList(varQualifiers, newNonVarQualifiers);
        }


        private QualifiersList withVarQualifier(final Qualifier qualifier) {
            final List<Qualifier> newVarQualifiers = new ArrayList<>(varQualifiers);
            newVarQualifiers.add(qualifier);
            return new QualifiersList(newVarQualifiers, nonVarQualifiers);
        }

        private QualifiersList withQualifiersList(final QualifiersList in) {
            final List<Qualifier> newNonVarQualifiers = new ArrayList<>(this.nonVarQualifiers);
            newNonVarQualifiers.addAll(in.nonVarQualifiers);
            final List<Qualifier> newVarQualifiers = new ArrayList<>(this.varQualifiers);
            newVarQualifiers.addAll(in.varQualifiers);
            return new QualifiersList(newVarQualifiers, newNonVarQualifiers);
        }

        public List<Qualifier> getNonVarQualifiers() {
            return nonVarQualifiers;
        }

        public List<Qualifier> getVarQualifiers() {
            return varQualifiers;
        }

        @Override
        public String toString() {
            return String.format("(Var: %s, NonVar: %s)", varQualifiers, nonVarQualifiers);
        }
    }
}

