/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg.compiler;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.vrg.IRColumn;
import com.vrg.IRContext;
import com.vrg.IRPrimaryKey;
import com.vrg.IRTable;
import org.jooq.Record;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Extracts the required Minizinc forall expressions and conditions from a FROM clause
 */
class FromExtractor extends DefaultTraversalVisitor<Void, Void> {
    private final Set<IRTable> tables = new HashSet<>();
    private final List<Expression> joinConditions = new ArrayList<>();
    private final IRContext irContext;

    FromExtractor(final IRContext irContext) {
        this.irContext = irContext;
    }

    @Override
    protected Void visitAliasedRelation(final AliasedRelation node, final Void context) {
        assert node.getRelation() instanceof Table : "Only table relations may have aliases";
        final IRTable irTable = irContext.getTable(((Table) node.getRelation()).getName().toString());

        // TODO: This duplicates code from WeaveModel in creating IRTable and IRColumn instances.
        final org.jooq.Table<? extends Record> table = irTable.isViewTable() ? null : irTable.getTable();
        final IRTable tableAlias = new IRTable(table, irTable.getName(), node.getAlias().getValue());

        // parse all fields
        for (final Map.Entry<String, IRColumn> entry: irTable.getIRColumns().entrySet()) {
            final IRColumn irColumn = entry.getValue();
            final IRColumn aliasIRColumn = irTable.isViewTable()
                                            ? new IRColumn(tableAlias, null, irColumn.getName())
                                            : new IRColumn(tableAlias, irColumn.getJooqField());
            tableAlias.addField(aliasIRColumn);
        }

        // After adding all the MnzFields to the table, we parse the table UniqueKey
        // and link the correspondent MnzFields as fields that compose the IRTable primary key
        if (!irTable.isViewTable()) {
            final IRPrimaryKey pk = new IRPrimaryKey(tableAlias, table.getPrimaryKey());
            tableAlias.setPrimaryKey(pk);
        }
        tables.add(tableAlias);
        irContext.addAliasedOrViewTable(tableAlias);
        return null;
    }

    @Override
    protected Void visitTable(final Table node, final Void context) {
        final IRTable irTable = irContext.getTable(node.getName().toString());
        tables.add(irTable);
        return null;
    }

    @Override
    protected Void visitJoin(final Join node, final Void context) {
        final Optional<JoinCriteria> criteria = node.getCriteria();
        if (criteria.isPresent()) {
            final JoinCriteria joinCriteria = criteria.get();
            if (joinCriteria instanceof JoinOn) {
                joinConditions.add(((JoinOn) joinCriteria).getExpression());
            }
            else {
                throw new UnsupportedOperationException("We only support 'join on', " +
                        "not natural join or join using");
            }
        }
        return super.visitJoin(node, context);
    }

    @Override
    protected Void visitSubqueryExpression(final SubqueryExpression node, final Void context) {
        return null;
    }

    Set<IRTable> getTables() {
        return ImmutableSet.copyOf(tables);
    }

    List<Expression> getJoinConditions() {
        return ImmutableList.copyOf(joinConditions);
    }
}
