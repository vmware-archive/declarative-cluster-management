/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Identifies and adds IRColumns to a given IRTable (viewTable) by scanning the select items in a particular view.
 */
class IRColumnsFromSelectItems extends SqlBasicVisitor<Void> {
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
    public Void visit(final SqlCall call) {
        if (call.getKind() == SqlKind.AS) {
            final String alias = ((SqlIdentifier) call.operand(1)).getSimple();
            LOG.warn("Guessing FieldType for column {} in non-constraint view {} to be INT",
                    alias, viewTable.getName());
            final IRColumn.FieldType intType = IRColumn.FieldType.INT;
            final IRColumn newColumn = new IRColumn(viewTable, null, intType, alias);
            viewTable.addField(newColumn);
            return null;
        }
        throw new IllegalArgumentException(call.toString());
    }

    @Override
    public Void visit(final SqlIdentifier id) {
        System.out.println(id);
        if (id.isStar()) {
            tablesReferencedInView.forEach(
                    table -> table.getIRColumns().forEach((fieldName, irColumn) -> viewTable.addField(irColumn))
            );
        } else if (id.isSimple()) {
            System.out.println("IS SIMPLE");
            final IRColumn columnIfUnique = irContext.getColumnIfUnique(id.getSimple(), tablesReferencedInView);
            final IRColumn newColumn = new IRColumn(viewTable, null, columnIfUnique.getType(),
                                                    columnIfUnique.getName());
            viewTable.addField(newColumn);
        } else {
            System.out.println("IS neither simple nor star");
            final IRColumn irColumn = TranslateViewToIR.getIRColumnFromDereferencedExpression(id, irContext);
            final IRColumn newColumn = new IRColumn(viewTable, null, irColumn.getType(),
                                                    irColumn.getName());
            viewTable.addField(newColumn);
        }
        return super.visit(id);
    }

    //    @Override
//    protected Void visitSingleColumn(final SingleColumn node, final Optional<String> context) {
//        final int before = viewTable.getIRColumns().size();
//        super.visitSingleColumn(node, node.getAlias().map(Identifier::getValue));
//        if (viewTable.getIRColumns().size() == before) {
//            // Was neither an identifier nor a dereference expression.
//            // We therefore assume its a supported expression, but require
//            // that it have an alias
//            LOG.warn("Guessing FieldType for column {} in non-constraint view {} to be INT",
//                     node.getAlias(), viewTable.getName());
//            final String alias = node.getAlias().orElseThrow().getValue();
//            final IRColumn.FieldType intType = IRColumn.FieldType.INT;
//            final IRColumn newColumn = new IRColumn(viewTable, null, intType, alias);
//            viewTable.addField(newColumn);
//        }
//        return null;
//    }
//
//    protected Void visitAllColumns(final AllColumns node, final Optional<String> context) {
//        tablesReferencedInView.forEach(
//                table -> table.getIRColumns().forEach((fieldName, irColumn) -> viewTable.addField(irColumn))
//        );
//        return null;
//    }
//
//    protected Void visitIdentifier(final Identifier node, final Optional<String> context) {
//        final IRColumn columnIfUnique = irContext.getColumnIfUnique(node.toString(), tablesReferencedInView);
//        final IRColumn newColumn = new IRColumn(viewTable, null, columnIfUnique.getType(),
//                context.orElse(columnIfUnique.getName()));
//        viewTable.addField(newColumn);
//        return null;
//    }
//
//    protected Void visitDereferenceExpression(final DereferenceExpression node, final Optional<String> context) {
//        final IRColumn irColumn = TranslateViewToIR.getIRColumnFromDereferencedExpression(node, irContext);
//        final IRColumn newColumn = new IRColumn(viewTable, null, irColumn.getType(),
//                context.orElse(irColumn.getName()));
//        viewTable.addField(newColumn);
//        return null;
//    }
}