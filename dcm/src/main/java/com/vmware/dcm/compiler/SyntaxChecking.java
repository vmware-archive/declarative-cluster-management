/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.ViewsWithAnnotations;

import javax.annotation.Nullable;

/*
 * Checks if the parsed AST only uses the supported subset of SQL to specify policies
 */
public class SyntaxChecking extends AstVisitor<Boolean, Void> {
    private final ViewsWithAnnotations view;
    private final String componentType;
    @Nullable Node lastTraversedNode = null;

    SyntaxChecking(final ViewsWithAnnotations view, final String componentType) {
        this.view = view;
        this.componentType = componentType;
    }

    @Override
    public Boolean process(final Node node, @Nullable final Void context) {
        this.lastTraversedNode = node;
        final Boolean ret = super.process(node, context);
        if (ret == null || !ret) {
            final NodeLocation nodeLocation = lastTraversedNode.getLocation().get();
            final String[] lines = componentType.equals("CREATE VIEW") ? view.getInputView().split("\n")
                                                : view.getInputConstraint().split("\n");
            final int lineNo = nodeLocation.getLineNumber() - 1;
            final int colNo = nodeLocation.getColumnNumber();
            final String faultyLine = lines[lineNo];
            final String error = String.format("---> Unexpected AST type %s (%s)",
                                               lastTraversedNode.getClass(), lastTraversedNode);
            final String withUnderline = faultyLine.substring(0, colNo - 1) + "\033[4m" +
                                      faultyLine.substring(colNo - 1) +
                                      String.format("%13s", "") + "\033[0m" + String.format("%10s", error);
            lines[lineNo] = withUnderline;
            final String inputStringWithUnderlineAndError = String.join("\n", lines);
            final String full = String.format("Unsupported SQL syntax in view \"%s\":" +
                                              "%n%s", view.getCreateView().getName(), inputStringWithUnderlineAndError);
            throw new ModelException(full);
        }
        return ret;
    }

    @Override
    protected Boolean visitCreateView(final CreateView node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitAliasedRelation(final AliasedRelation node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitAllColumns(final AllColumns node, final Void context) {
        return true;
    }

    @Override
    protected Boolean visitGroupBy(final GroupBy node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitSimpleGroupBy(final SimpleGroupBy node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitTable(final Table node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitJoin(final Join node, final Void context) {
        return node.getType().equals(Join.Type.INNER) && processChildren(node);
    }

    @Override
    protected Boolean visitQuery(final Query node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitQuerySpecification(final QuerySpecification node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitSelect(final Select node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitLogicalBinaryExpression(final LogicalBinaryExpression node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitComparisonExpression(final ComparisonExpression node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitArithmeticBinary(final ArithmeticBinaryExpression node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitArithmeticUnary(final ArithmeticUnaryExpression node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitExists(final ExistsPredicate node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitInPredicate(final InPredicate node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitFunctionCall(final com.facebook.presto.sql.tree.FunctionCall node,
                                     final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitDereferenceExpression(final DereferenceExpression node, final Void context) {
        return true;
    }

    @Override
    protected Boolean visitSubqueryExpression(final SubqueryExpression node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitSingleColumn(final SingleColumn node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitNotExpression(final NotExpression node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitIsNullPredicate(final IsNullPredicate node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitIsNotNullPredicate(final IsNotNullPredicate node, final Void context) {
        return processChildren(node);
    }

    @Override
    protected Boolean visitLiteral(final com.facebook.presto.sql.tree.Literal node, final Void context) {
        return true;
    }

    @Override
    protected Boolean visitStringLiteral(final StringLiteral node, final Void context) {
        return true;
    }

    @Override
    protected Boolean visitLongLiteral(final LongLiteral node, final Void context) {
        return true;
    }

    @Override
    protected Boolean visitBooleanLiteral(final BooleanLiteral node, final Void context) {
        return true;
    }

    @Override
    protected Boolean visitIdentifier(final Identifier node, final Void context) {
        return true;
    }

    private Boolean processChildren(final Node node) {
        return node.getChildren().stream().map(this::process).reduce(true, (a, b) -> a && b);
    }

    public static void apply(final ViewsWithAnnotations view) {
        check(view, view.getCreateView(), "CREATE VIEW");
        view.getCheckExpression().ifPresent(expr -> check(view, expr, "CHECK"));
        view.getMaximizeExpression().ifPresent(expr -> check(view, expr, "MAXIMIZE"));
    }

    private static void check(final ViewsWithAnnotations view, final Node part, final String partType) {
        final SyntaxChecking validQuery = new SyntaxChecking(view, partType);
        validQuery.process(part);
    }
}