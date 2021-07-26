/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.Set;

/**
 * Collects table names for every table referenced within a query.
 */
public class ExtractAccessedTables extends SqlBasicVisitor<Void> {
    final Set<String> tableNames;

    ExtractAccessedTables(final Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public Void visit(final SqlCall call) {
        if (call instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect) call;
            final SqlNode node = select.getFrom();
            assert node != null;
            visitInner(node);
        }
        return super.visit(call);
    }

    private void visitInner(final SqlNode node) {
        switch (node.getKind()) {
            case IDENTIFIER:
                tableNames.add(((SqlIdentifier) node).getSimple());
                break;
            case AS:
                final SqlNode[] operands = ((SqlBasicCall) node).getOperands();
                assert operands != null;
                final SqlIdentifier relation = (SqlIdentifier) operands[0];
                assert relation != null;
                tableNames.add(relation.getSimple());
                break;
            case JOIN:
                final SqlJoin join = (SqlJoin) node;
                visitInner(join.getLeft());
                visitInner(join.getRight());
                break;
            default:
                break;
        }
    }
}
