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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ExtractConstraintInQuery extends SqlBasicVisitor<Void> {
    final Map<String, Map<String, String>> inClause;
    final Map<String, Map<String, String>> notInClause;
    final Map<String, List<String>> consts;


    ExtractConstraintInQuery(final Map<String, Map<String, String>> inClause,
                             final Map<String, Map<String, String>> notInClause,
                             final Map<String, List<String>> consts) {
        this.inClause = inClause;
        this.notInClause = notInClause;
        this.consts = consts;
    }

    @Override
    public Void visit(final SqlCall call) {
        final SqlOperator op = call.getOperator();
        final List<SqlNode> operands = call.getOperandList();
        // Not safe: (controllable__var NOT IN (const query)) OR (predicate on non-var)
        if (op.getKind() == SqlKind.OR) {
            final String clause1 = operands.get(0).toString().toLowerCase();
            final String clause2 = operands.get(1).toString().toLowerCase();
            // No variable
            if (!clause1.contains("controllable__") && !clause2.contains("controllable__")) {
                return null;
            } else if (clause1.contains("controllable__") && !clause2.contains("controllable__")) {
                if (isExclusion((SqlCall) operands.get(0))) {
                    return null;
                }
            } else if (clause2.contains("controllable__") && !clause1.contains("controllable__")) {
                if (isExclusion((SqlCall) operands.get(1))) {
                    return null;
                }
            }
        }

        // controllable__var (NOT) IN (SELECT subquery) or
        // (NOT) CONTAINS (, controllable__var)
        if (isExclusion(call) || isInclusion(call)) {
            final SqlSelect select = (SqlSelect) operands.get(1);
            final String var = getVarName((SqlIdentifier) operands.get(0));
            parseSelect(var, select, op);
        } else if (op.getKind() == SqlKind.EQUALS && operands.size() == 2 && isVariable(operands.get(0))) {
            final SqlIdentifier id = (SqlIdentifier) operands.get(0);
            if (!id.isSimple()) {
                return null;
            }
            final String var = getVarName(id);
            final String c = operands.get(1).toString();
            final List<String> vals = consts.getOrDefault(var, new ArrayList<>());
            if (!vals.contains(c)) {
                vals.add(c);
            }
            consts.put(var, vals);
        }
        return super.visit(call);
    }

    private boolean isExclusion(final SqlCall call) {
        final SqlOperator op = call.getOperator();
        final List<SqlNode> operands = call.getOperandList();
        // NOT CONTAINS(fieldname, var)
        if (op.getKind() == SqlKind.NOT && operands.size() == 1) {
            final SqlCall c = (SqlCall) operands.get(0);
            final SqlOperator op1 = c.getOperator();
            final List<SqlNode> operandList = c.getOperandList();
            return (op1.getName().equals("CONTAINS") && operandList.size() == 2 && isVariable(operandList.get(1)));
        }
        // var NOT IN (select )
        else if (op.getKind() == SqlKind.NOT_IN) {
            return operands.size() == 2 && isVariable(operands.get(0))
                    && operands.get(1) instanceof SqlSelect;
        } else {
            return false;
        }
    }

    private boolean isInclusion(final SqlCall call) {
        final SqlOperator op = call.getOperator();
        final List<SqlNode> operands = call.getOperandList();
        if (op.getName().equals("CONTAINS")) {
            return operands.size() == 2 && isVariable(operands.get(1));
        } else if (op.getKind() == SqlKind.IN) {
            return operands.size() == 2 && isVariable(operands.get(0))
                    && operands.get(1) instanceof SqlSelect;
        } else {
            return false;
        }
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

    private void parseSelect(final String var, final SqlSelect select, final SqlOperator op) {
        final SqlNode from = select.getFrom();
        final SqlNodeList names = select.getSelectList();
        assert from != null;
        assert names.size() == 1;

        // Ignore SELF JOIN cases
        // e.g., controllable__var (NOT) IN (SELECT controllable__var ...)
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
            if (op.getKind() == SqlKind.IN) {
                if (!inClause.containsKey(var)) {
                    final Map<String, String> clause = new HashMap<>();
                    inClause.put(var, clause);
                }
                inClause.get(var).put(tableName, fieldName);
            } else {
                if (!notInClause.containsKey(var)) {
                    final Map<String, String> clause = new HashMap<>();
                    notInClause.put(var, clause);
                }
                notInClause.get(var).put(tableName, fieldName);
            }
        }
    }


}
