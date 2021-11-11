/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ExtractConstraintInQuery extends SqlBasicVisitor<Void> {
    final Map<String, Map<String, String>> selectClause;

    ExtractConstraintInQuery(final Map<String, Map<String, String>> selectClause) {
        this.selectClause = selectClause;
    }

    @Override
    public Void visit(final SqlCall call) {
        final SqlOperator op = call.getOperator();
        final List<SqlNode> operands = call.getOperandList();
        // controllable__var IN (SELECT subquery)
        //if ((op.getKind() == SqlKind.IN || op.getKind() == SqlKind.NOT_IN) &&
        if ((op.getKind() == SqlKind.IN) &&
            operands.size() == 2 && isVariable(operands.get(0)) && operands.get(1) instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect) operands.get(1);
            final String var = getVarName((SqlIdentifier) operands.get(0));
            if (!selectClause.containsKey(var)) {
                final Map<String, String> clause = new HashMap<>();
                selectClause.put(var, clause);
            }
            parseSelect(var, select);
        }
        return super.visit(call);
    }

    private boolean isVariable(final SqlNode node) {
        if (!(node instanceof SqlIdentifier)) {
            return false;
        }
        final SqlIdentifier id = (SqlIdentifier) node;
        if (id.isSimple()) {
            return id.getSimple().toLowerCase().startsWith("controllable__");
        } else if (id.names.size() == 2) {
            return id.names.get(1).toLowerCase().startsWith("controllable__");
        }
        return false;
    }

    private String getVarName(final SqlIdentifier id) {
        if (id.isSimple()) {
            return id.getSimple().toLowerCase().substring(14);
        } else {
            return id.names.get(1).toLowerCase().substring(14);
        }
    }

    private String getTableName(final SqlNode from) {
        final SqlIdentifier id;
        if (from.getKind() == SqlKind.AS) {
            id = ((SqlBasicCall) from).operand(0);
        } else {
            id = ((SqlIdentifier) from);
        }
        return id.getSimple();
    }

    private void parseSelect(final String var, final SqlSelect select) {
        final SqlNode from = select.getFrom();
        final SqlNodeList names = select.getSelectList();
        assert from != null;
        assert names.size() == 1;

        // Ignore SELF JOIN cases
        // e.g., controllable__var IN (SELECT controllable__var ...)
        if (isVariable(names.get(0))) {
            return;
        }

        // Extract table and field name
        if (from.getKind() == SqlKind.IDENTIFIER || from.getKind() == SqlKind.AS) {
            final String tableName = getTableName(from);
            final SqlIdentifier name = (SqlIdentifier) names.get(0);
            String fieldName = "";
            if (name.isSimple()) {
                // SELECT name FROM table
                fieldName = name.getSimple();
            } else if (name.names.size() == 2 && name.names.get(0).equals(tableName)) {
                // SELECT table.name FROM table
                fieldName = name.names.get(1);
            }
            selectClause.get(var).put(tableName, fieldName);
        }
    }


}
