/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.List;
import java.util.Map;


public class ExtractConstraintInQuery extends SqlBasicVisitor<Void> {
    final Map<String, String> selectClause;

    ExtractConstraintInQuery(Map<String, String> selectClause) {
        this.selectClause = selectClause;
    }

    @Override
    public Void visit(final SqlCall call) {
        SqlOperator op = call.getOperator();
        List<SqlNode> operands = call.getOperandList();
        // controllable__var IN (SELECT subquery)
        if ((op.getKind() == SqlKind.IN || op.getKind() == SqlKind.NOT_IN) &&
            operands.size() == 2 && isVariable(operands.get(0)) && operands.get(1) instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect) operands.get(1);
            parseSelect(select);
        }
        return super.visit(call);
    }

    private boolean isVariable(SqlNode node) {
        if (!(node instanceof SqlIdentifier)) return false;
        SqlIdentifier id = (SqlIdentifier) node;
        if (id.isSimple()) {
            return id.getSimple().toLowerCase().startsWith("controllable__");
        } else if (id.names.size() == 2) {
            return id.names.get(1).toLowerCase().startsWith("controllable__");
        }
        return false;
    }

    private void parseSelect(final SqlSelect select) {
        final SqlNode from = select.getFrom();
        SqlNodeList names = select.getSelectList();
        assert from != null;
        assert names.size() == 1;

        // Ignore SELF JOIN cases
        // e.g., controllable__var IN (SELECT controllable__var ...)
        if (isVariable(names.get(0))) return;

        // Extract table and field name
        if (from.getKind() == SqlKind.IDENTIFIER) {
            String tableName = ((SqlIdentifier) from).getSimple();
            SqlIdentifier name = (SqlIdentifier) names.get(0);
            String fieldName = "";
            if (name.isSimple()) {
                // SELECT name FROM table
                fieldName = name.getSimple();
            } else if (name.names.size() == 2 && name.names.get(0).equals(tableName)) {
                // SELECT table.name FROM table
                fieldName = name.names.get(1);
            }
            selectClause.put(tableName, fieldName);
        }
    }


}
