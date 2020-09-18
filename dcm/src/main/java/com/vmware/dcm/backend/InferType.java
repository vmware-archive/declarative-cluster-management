/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.vmware.dcm.compiler.monoid.IsNotNullPredicate;
import com.vmware.dcm.compiler.monoid.MonoidFunction;
import com.vmware.dcm.compiler.monoid.MonoidVisitor;
import com.vmware.dcm.compiler.monoid.VoidType;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.compiler.monoid.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.monoid.ColumnIdentifier;
import com.vmware.dcm.compiler.monoid.ExistsPredicate;
import com.vmware.dcm.compiler.monoid.Expr;
import com.vmware.dcm.compiler.monoid.GroupByComprehension;
import com.vmware.dcm.compiler.monoid.Head;
import com.vmware.dcm.compiler.monoid.IsNullPredicate;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import com.vmware.dcm.compiler.monoid.MonoidLiteral;
import com.vmware.dcm.compiler.monoid.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class InferType extends MonoidVisitor<String, VoidType> {
    private static final Logger LOG = LoggerFactory.getLogger(InferType.class);
    private final Map<String, String> viewTupleTypeParameters;

    InferType(final Map<String, String> viewTupleTypeParameters) {
        this.viewTupleTypeParameters = viewTupleTypeParameters;
    }

    public String visit(final Expr expr) {
        return super.visit(expr, VoidType.getAbsent());
    }

    @Override
    protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final VoidType context) {
        final String leftType = visit(node.getLeft());
        final String rightType = visit(node.getRight());
        if (leftType.equals("IntVar") || rightType.equals("IntVar")) {
            return "IntVar";
        }
        switch (node.getOperator()) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IN:
            case OR:
            case AND:
            case CONTAINS:
                return "Boolean";
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULUS:
                return "Integer";
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected String visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
        final Head head = node.getComprehension().getHead();
        if (head.getSelectExprs().size() == 1) {
            final String type = visit(head.getSelectExprs().get(0), context);
            LOG.warn("Returning type of sub-query {} as {}", node, type);
            return type;
        }
        throw new UnsupportedOperationException("Do not know type of subquery");
    }

    @Override
    protected String visitMonoidComprehension(final MonoidComprehension node, final VoidType context) {
        final Head head = node.getHead();
        if (head.getSelectExprs().size() == 1) {
            final String type = visit(head.getSelectExprs().get(0), context);
            LOG.warn("Returning type of sub-query {} as {}", node, type);
            return type;
        }
        throw new UnsupportedOperationException("Do not know type of subquery");
    }

    @Override
    protected String visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        return typeStringFromColumn(node);
    }

    @Override
    protected String visitMonoidFunction(final MonoidFunction node, final VoidType context) {
        if (node.getFunction().equals(MonoidFunction.Function.CAPACITY_CONSTRAINT)) {
            return "IntVar";
        }
        Preconditions.checkArgument(node.getArgument().size() == 1);
        return visit(node.getArgument().get(0), context);
    }

    @Override
    protected String visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
        // TODO: This is incomplete. It can be boolean if node.getArgument() is const.
        return "IntVar";
    }

    @Override
    protected String visitIsNullPredicate(final IsNullPredicate node, final VoidType context) {
        return visit(node.getArgument(), context).equals("IntVar") ? "IntVar" : "Boolean";
    }

    @Override
    protected String visitIsNotNullPredicate(final IsNotNullPredicate node, final VoidType context) {
        return visit(node.getArgument(), context).equals("IntVar") ? "IntVar" : "Boolean";
    }

    @Override
    protected String visitUnaryOperator(final UnaryOperator node, final VoidType context) {
        final String type = visit(node.getArgument(), context);
        switch (node.getOperator()) {
            case NOT:
                return type.equals("IntVar") ? "IntVar" : "Boolean";
            case MINUS:
            case PLUS:
                return type.equals("IntVar") ? "IntVar" : "Integer";
            default:
                throw new IllegalArgumentException(node.toString());
        }
    }

    @Override
    protected String visitMonoidLiteral(final MonoidLiteral node, final VoidType context) {
        if (node.getValue() instanceof String) {
            return "String";
        } else if (node.getValue() instanceof Integer) {
            return "Integer";
        } else if (node.getValue() instanceof Boolean) {
            return "Boolean";
        } else if (node.getValue() instanceof Long) {
            return "Integer";
        }
        return super.visitMonoidLiteral(node, context);
    }

    private  String typeStringFromColumn(final ColumnIdentifier node) {
        if (node.getField().isControllable()) {
            return "IntVar";
        }
        final String convertedName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, node.getTableName());
        if (viewTupleTypeParameters.containsKey(convertedName) &&
                viewTupleTypeParameters.get(convertedName).contains("IntVar")) {
            LOG.warn("Inferring type for column {} as IntVar", node);
            return "IntVar";
        }
        return typeStringFromColumn(node.getField());
    }

    static String typeStringFromColumn(final IRColumn column) {
        switch (column.getType()) {
            case STRING:
                return  "String";
            case BOOL:
                return  "Boolean";
            case INT:
                return  "Integer";
            case FLOAT:
                return  "Float";
            case ARRAY:
                return  "Object[]";
            default:
                throw new IllegalArgumentException();
        }
    }

    // TODO: passing viewTupleTypeParameters makes this class tightly coupled with OrToolsSolver.
    static String forExpr(final Expr expr, final Map<String, String> viewTupleTypeParameters) {
        final InferType visitor = new InferType(viewTupleTypeParameters);
        final String result = visitor.visit(expr);
        assert !result.isEmpty();
        return result;
    }
}