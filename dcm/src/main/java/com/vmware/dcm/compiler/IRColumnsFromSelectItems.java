/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.SingleColumn;
import com.vmware.dcm.compiler.IRColumn;
import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.compiler.IRTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Identifies and adds IRColumns to a given IRTable (viewTable) by scanning the select items in a particular view.
 */
class IRColumnsFromSelectItems extends DefaultTraversalVisitor<Void, Optional<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(IRColumnsFromSelectItems.class);
    private final IRContext irContext;
    private final IRTable viewTable;
    private final Set<IRTable> tablesReferencedInView;

    IRColumnsFromSelectItems(final IRContext irContext, final IRTable viewTable,
                             final Set<IRTable> tablesReferencedInView) {
        this.irContext = irContext;
        this.viewTable = viewTable;
        this.tablesReferencedInView = tablesReferencedInView;
    }

    @Override
    protected Void visitSingleColumn(final SingleColumn node, final Optional<String> context) {
        final int before = viewTable.getIRColumns().size();
        super.visitSingleColumn(node, node.getAlias().map(Identifier::getValue));
        if (viewTable.getIRColumns().size() == before) {
            // Was neither an identifier nor a dereference expression.
            // We therefore assume its a supported expression, but require
            // that it have an alias
            LOG.warn("Guessing FieldType for column {} in non-constraint view {} to be INT",
                     node.getAlias(), viewTable.getName());
            final String alias = node.getAlias().orElseThrow().getValue();
            final IRColumn.FieldType intType = IRColumn.FieldType.INT;
            final IRColumn newColumn = new IRColumn(viewTable, null, intType, alias);
            viewTable.addField(newColumn);
        }
        return null;
    }

    @Override
    protected Void visitAllColumns(final AllColumns node, final Optional<String> context) {
        tablesReferencedInView.forEach(
                table -> table.getIRColumns().forEach((fieldName, irColumn) -> viewTable.addField(irColumn))
        );
        return null;
    }

    @Override
    protected Void visitIdentifier(final Identifier node, final Optional<String> context) {
        final IRColumn columnIfUnique = irContext.getColumnIfUnique(node.toString(), tablesReferencedInView);
        final IRColumn newColumn = new IRColumn(viewTable, null, columnIfUnique.getType(),
                context.orElse(columnIfUnique.getName()));
        viewTable.addField(newColumn);
        return null;
    }

    @Override
    protected Void visitDereferenceExpression(final DereferenceExpression node, final Optional<String> context) {
        final IRColumn irColumn = TranslateViewToIR.getIRColumnFromDereferencedExpression(node, irContext);
        final IRColumn newColumn = new IRColumn(viewTable, null, irColumn.getType(),
                context.orElse(irColumn.getName()));
        viewTable.addField(newColumn);
        return null;
    }
}