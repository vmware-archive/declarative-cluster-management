/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.Head;
import com.vmware.dcm.compiler.ir.IRVisitor;
import com.vmware.dcm.compiler.ir.IsNotNullPredicate;
import com.vmware.dcm.compiler.ir.IsNullPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.UnaryOperator;
import com.vmware.dcm.compiler.ir.VoidType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class InferType extends IRVisitor<JavaType, VoidType> {
    private static final Logger LOG = LoggerFactory.getLogger(InferType.class);
    private final Map<String, JavaTypeList> viewTupleTypeParameters;

    InferType(final Map<String, JavaTypeList> viewTupleTypeParameters) {
        this.viewTupleTypeParameters = viewTupleTypeParameters;
    }

    public JavaType visit(final Expr expr) {
        return super.visit(expr, VoidType.getAbsent());
    }

    @Override
    protected JavaType visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final VoidType context) {
        final JavaType leftType = visit(node.getLeft());
        final JavaType rightType = visit(node.getRight());
        if (leftType == JavaType.IntVar || rightType == JavaType.IntVar) {
            return JavaType.IntVar;
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
                return JavaType.Boolean;
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULUS:
                return JavaType.Integer;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected JavaType visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
        final Head head = node.getComprehension().getHead();
        if (head.getSelectExprs().size() == 1) {
            final JavaType type = visit(head.getSelectExprs().get(0), context);
            LOG.warn("Returning type of sub-query {} as {}", node, type);
            return type;
        }
        throw new UnsupportedOperationException("Do not know type of subquery");
    }

    @Override
    protected JavaType visitListComprehension(final ListComprehension node, final VoidType context) {
        final Head head = node.getHead();
        if (head.getSelectExprs().size() == 1) {
            final JavaType type = visit(head.getSelectExprs().get(0), context);
            LOG.warn("Returning type of sub-query {} as {}", node, type);
            return type;
        }
        throw new UnsupportedOperationException("Do not know type of subquery");
    }

    @Override
    protected JavaType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        return typeFromColumn(node);
    }

    @Override
    protected JavaType visitFunctionCall(final FunctionCall node, final VoidType context) {
        if (node.getFunction().equals(FunctionCall.Function.CAPACITY_CONSTRAINT)) {
            return JavaType.IntVar;
        }
        Preconditions.checkArgument(node.getArgument().size() == 1);
        return visit(node.getArgument().get(0), context);
    }

    @Override
    protected JavaType visitExistsPredicate(final ExistsPredicate node, final VoidType context) {
        // TODO: This is incomplete. It can be boolean if node.getArgument() is const.
        return visit(node.getArgument(), context) == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
    }

    @Override
    protected JavaType visitIsNullPredicate(final IsNullPredicate node, final VoidType context) {
        return visit(node.getArgument(), context) == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
    }

    @Override
    protected JavaType visitIsNotNullPredicate(final IsNotNullPredicate node, final VoidType context) {
        return visit(node.getArgument(), context) == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
    }

    @Override
    protected JavaType visitUnaryOperator(final UnaryOperator node, final VoidType context) {
        final JavaType type = visit(node.getArgument(), context);
        switch (node.getOperator()) {
            case NOT:
                return type == JavaType.IntVar ? JavaType.IntVar : JavaType.Boolean;
            case MINUS:
            case PLUS:
                return type;
            default:
                throw new IllegalArgumentException(node.toString());
        }
    }

    @Override
    protected JavaType visitLiteral(final Literal node, final VoidType context) {
        if (node.getValue() instanceof String) {
            return JavaType.String;
        } else if (node.getValue() instanceof Integer) {
            return JavaType.Integer;
        } else if (node.getValue() instanceof Boolean) {
            return JavaType.Boolean;
        } else if (node.getValue() instanceof Long) {
            return JavaType.Long;
        }
        return super.visitLiteral(node, context);
    }

    private JavaType typeFromColumn(final ColumnIdentifier node) {
        if (node.getField().isControllable()) {
            return JavaType.IntVar;
        }
        final String convertedName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, node.getTableName());
        if (viewTupleTypeParameters.containsKey(convertedName) &&
                viewTupleTypeParameters.get(convertedName).contains(JavaType.IntVar)) {
            LOG.warn("Inferring type for column {} as IntVar", node);
            return JavaType.IntVar;
        }
        return typeFromColumn(node.getField());
    }

    static JavaType typeFromColumn(final IRColumn column) {
        switch (column.getType()) {
            case STRING:
                return  JavaType.String;
            case BOOL:
                return  JavaType.Boolean;
            case LONG:
                return JavaType.Long;
            case INT:
                return  JavaType.Integer;
            case FLOAT:
                return  JavaType.Float;
            case ARRAY:
                return  JavaType.ObjectArray;
            default:
                throw new IllegalArgumentException(column.toString());
        }
    }

    // TODO: passing viewTupleTypeParameters makes this class tightly coupled with TupleMetadata.
    static JavaType forExpr(final Expr expr, final Map<String, JavaTypeList> viewTupleTypeParameters) {
        final InferType visitor = new InferType(viewTupleTypeParameters);
        return visitor.visit(expr);
    }

    static String toTypeString(final JavaType type) {
        return type.typeString();
    }
}