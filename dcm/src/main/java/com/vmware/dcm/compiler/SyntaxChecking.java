/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.vmware.dcm.ModelException;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.util.SqlBasicVisitor;

/*
 * Checks if the parsed AST only uses the supported subset of SQL to specify policies
 */
public class SyntaxChecking extends SqlBasicVisitor<Boolean> {

    @Override
    public Boolean visit(final SqlCall call) {
        if (isAllowedKind(call)) {
            return super.visit(call);
        }
        throw new ModelException("Unexpected AST type " + call);
    }

    @Override
    public Boolean visit(final SqlIdentifier id) {
        if (id.isStar() || id.isSimple() || id.names.size() == 2) {
            return super.visit(id);
        }
        throw new ModelException("Unexpected AST type " + id);
    }

    @Override
    public Boolean visit(final SqlLiteral literal) {
        return true;
    }

    private boolean isAllowedKind(final SqlCall call) {
        switch (call.getOperator().getKind()) {
            case OTHER_FUNCTION:
                try {
                    FunctionCall.Function.valueOf(call.getOperator().getName().toUpperCase());
                    return true;
                } catch (final IllegalArgumentException e) {
                    return false;
                }
            case JOIN:
                final SqlJoin join = (SqlJoin) call;
                if (join.getJoinType() == JoinType.INNER) {
                    return true;
                } else {
                    // We throw an exception here because SqlJoin.toString() is buggy
                    // https://issues.apache.org/jira/browse/CALCITE-4401
                    throw new ModelException("Unexpected AST type " + join.getLeft() + " " + join.getJoinType()
                            + " JOIN " + join.getRight() + " ON (" + join.getCondition() + ")");
                }
            case SELECT:
            case IN:
            case EXISTS:
            case NOT:
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case MOD:
            case EQUALS:
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
            case NOT_EQUALS:
            case AND:
            case OR:
            case MINUS_PREFIX:
            case PLUS_PREFIX:
            case IS_NULL:
            case IS_NOT_NULL:
            case AS:
                return true;
            default:
                return false;
        }
    }

    public static void apply(final SqlCreateConstraint ddl) {
        final SyntaxChecking validQuery = new SyntaxChecking();
        ddl.getQuery().accept(validQuery);
        ddl.getConstraint().ifPresent(e -> e.accept(validQuery));
    }
}