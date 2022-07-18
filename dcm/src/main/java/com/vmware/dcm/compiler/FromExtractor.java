/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.jooq.Record;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts IRTables and filtering conditions from a FROM clause
 */
class FromExtractor extends SqlBasicVisitor<Void> {
    // Using a LinkedHashSet is necessary to have a deterministic order of tables in the IR
    private final Set<IRTable> tables = new LinkedHashSet<>();
    private final List<SqlNode> joinConditions = new ArrayList<>();
    private final IRContext irContext;
    private boolean seen = false;

    FromExtractor(final IRContext irContext) {
        this.irContext = irContext;
    }

    @Override
    public Void visit(final SqlCall call) {
        if (!seen && call instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect) call;
            final SqlNode from = select.getFrom();
            assert from != null;
            visitRelation(from);
            seen = true;
        }
        return super.visit(call);
    }

    private void visitRelation(final SqlNode node) {
        switch (node.getKind()) {
            case IDENTIFIER:
                tables.add(toIrTable(node));
                break;
            case AS:
                final List<SqlNode> operands = ((SqlBasicCall) node).getOperandList();
                assert operands != null;
                final SqlIdentifier relation = (SqlIdentifier) operands.get(0);
                final SqlIdentifier alias = (SqlIdentifier) operands.get(1);
                assert relation != null;
                assert alias != null;
                final IRTable irTable = toIrTable(relation);
                // TODO: This duplicates code from Model in creating IRTable and IRColumn instances.
                final org.jooq.Table<? extends Record> table = irTable.isViewTable() ? null : irTable.getTable();
                final IRTable tableAlias = new IRTable(table, irTable.getName(), alias.getSimple());

                // parse all fields
                for (final Map.Entry<String, IRColumn> entry: irTable.getIRColumns().entrySet()) {
                    final IRColumn irColumn = entry.getValue();
                    final IRColumn aliasIRColumn = irTable.isViewTable()
                            ? new IRColumn(tableAlias, null, irColumn.getType(),
                            irColumn.getName())
                            : new IRColumn(tableAlias, irColumn.getJooqField());
                    tableAlias.addField(aliasIRColumn);
                }

                // After adding all the IRFields to the table, we parse the table UniqueKey
                // and link the correspondent IRFields as fields that compose the IRTable primary key
                if (!irTable.isViewTable()) {
                    final IRPrimaryKey pk = new IRPrimaryKey(tableAlias, table.getPrimaryKey());
                    tableAlias.setPrimaryKey(pk);
                }
                tables.add(tableAlias);
                irContext.addAliasedOrViewTable(tableAlias);
                break;
            case JOIN:
                final SqlJoin join = (SqlJoin) node;
                visitRelation(join.getLeft());
                visitRelation(join.getRight());
                joinConditions.add(join.getCondition());
                break;
            default:
                break;
        }
    }

    private IRTable toIrTable(final SqlNode identifier) {
        return irContext.getTable(((SqlIdentifier) identifier).getSimple());
    }

    Set<IRTable> getTables() {
        return ImmutableSet.copyOf(tables);
    }

    List<SqlNode> getJoinConditions() {
        return ImmutableList.copyOf(joinConditions);
    }
}
