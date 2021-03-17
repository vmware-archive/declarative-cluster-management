/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;


import com.vmware.dcm.compiler.UsesControllableFields;
import com.vmware.dcm.compiler.ir.GroupByQualifier;
import com.vmware.dcm.compiler.ir.IsNotNullPredicate;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.IRVisitor;
import com.vmware.dcm.compiler.ir.Qualifier;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicateWithAggregate;
import com.vmware.dcm.compiler.ir.CheckQualifier;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.IsNullPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.TableRowGenerator;
import com.vmware.dcm.compiler.ir.UnaryOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Separates qualifiers in an expression into vars and non-vars.
 */
public class GetVarQualifiers extends IRVisitor<GetVarQualifiers.QualifiersList, GetVarQualifiers.QualifiersList> {
    private final boolean skipAggregates;

    GetVarQualifiers(final boolean skipAggregates) {
        this.skipAggregates = skipAggregates;
    }

    public static QualifiersList apply(final Expr expr, final boolean skipAggregates) {
        final GetVarQualifiers visitor = new GetVarQualifiers(skipAggregates);
        return visitor.visit(expr);
    }

    static QualifiersList apply(final Expr expr) {
        return apply(expr, true);
    }

    public QualifiersList visit(final Expr expr) {
        return super.visit(expr, new GetVarQualifiers.QualifiersList());
    }

    @Override
    protected QualifiersList visitGroupByQualifier(final GroupByQualifier node,
                                                   final QualifiersList context) {
        return context.withNonVarQualifier(node);
    }

    @Override
    protected QualifiersList visitGroupByComprehension(final GroupByComprehension node, final QualifiersList context) {
        return context;
    }

    @Override
    protected QualifiersList visitListComprehension(final ListComprehension node, final QualifiersList context) {
        return context;
    }

    @Override
    protected QualifiersList visitTableRowGenerator(final TableRowGenerator node, final QualifiersList context) {
        return context.withNonVarQualifier(node);
    }

    @Override
    protected QualifiersList visitCheckExpression(final CheckQualifier node, final QualifiersList context) {
        if (usesControllableField(node.getExpr())) {
            return context.withVarQualifier(node);
        } else {
            return context.withNonVarQualifier(node);
        }
    }

    @Override
    protected QualifiersList visitUnaryOperator(final UnaryOperator node, final QualifiersList context) {
        // TODO: revisit the sub-types of Qualifiers. Not every qualifier needs to be a binary operator.
        if (node.getOperator().equals(UnaryOperator.Operator.NOT)) {
            final BinaryOperatorPredicate rewritten =
                    new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.EQUAL, node.getArgument(),
                            new Literal<>(false, Boolean.class));
            if (usesControllableField(node.getArgument())) {
                return context.withVarQualifier(rewritten);
            } else {
                return context.withNonVarQualifier(rewritten);
            }
        }
        return context;
    }

    @Override
    protected QualifiersList visitFunctionCall(final FunctionCall node, final QualifiersList context) {
        return context;
    }

    @Override
    protected QualifiersList visitIsNotNullPredicate(final IsNotNullPredicate node, final QualifiersList context) {
        return context;
    }

    @Override
    protected QualifiersList visitIsNullPredicate(final IsNullPredicate node, final QualifiersList context) {
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
            case CONTAINS:
            case OR: {
                // function expressions do not necessarily affect the arity of an outer expression.
                // We err on the conservative side for now.
                if ((usesControllableField(node.getLeft()) || usesControllableField(node.getRight()))
                        && involvesAggregateFunctions(node)) {
                    return context.withVarQualifier(checkForAggregate(node));
                }
                return context.withNonVarQualifier(checkForAggregate(node));
            }
            case AND: {
                final QualifiersList left = visit(node.getLeft(), context);
                final QualifiersList right = visit(node.getRight(), context);
                return left.withQualifiersList(right);
            }
            default:
                throw new RuntimeException("Missing case " + node.getOperator());
        }
    }

    private boolean involvesAggregateFunctions(final BinaryOperatorPredicate node) {
        if (skipAggregates) {
            return (!(node.getLeft() instanceof FunctionCall) && !(node.getRight() instanceof FunctionCall));
        }
        else {
            return true;
        }
    }

    private BinaryOperatorPredicate checkForAggregate(final BinaryOperatorPredicate node) {
        return node instanceof BinaryOperatorPredicateWithAggregate ?
                new BinaryOperatorPredicateWithAggregate(node) : node;
    }

    private boolean usesControllableField(final Expr expr) {
        final UsesControllableFields usesControllableFields = new UsesControllableFields();
        usesControllableFields.visit(expr);
        return usesControllableFields.usesControllableFields();
    }


    /**
     * Container to propagate var and non-var qualifiers
     */
    public static class QualifiersList {
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

