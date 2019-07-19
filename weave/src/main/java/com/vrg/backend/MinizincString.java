/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import com.vrg.IRColumn;
import com.vrg.IRTable;
import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.Expr;
import com.vrg.compiler.monoid.MonoidLiteral;

import java.util.Locale;

/**
 * Utility class that converts SQL types (table names, iterator names, literals, column names etc.)
 * into the corresponding strings used in Minizinc
 */
class MinizincString {
    static final String MNZ_AND = " /\\ ";
    static final char MNZ_OUTPUT_CSV_DELIMITER = ',';
    static final String MNZ_OUTPUT_TABLENAME_TAG = "\n!!";
    private static final String NUM_ROWS_NAME = "NUM_ROWS";

    static String tableNumRowsName(final IRTable table) {
        return tableNumRowsName(table.getName());
    }

    static String tableNumRowsName(final String tableName) {
        return String.format("%s__%s", tableName, NUM_ROWS_NAME);
    }

    /**
     * @return Returns this table qualified name
     */
    static String qualifiedName(final IRColumn field) {
        return String.format("%s__%s", field.getIRTable().getName(), field.getName());
    }

    static String headItemVariableName(final Expr expr) {
        if (expr.getAlias().isPresent()) {
            return expr.getAlias().get().toUpperCase(Locale.US);
        }
        else if (expr instanceof ColumnIdentifier) {
            final IRColumn field = ((ColumnIdentifier) expr).getField();
            return String.format("%s__%s", field.getIRTable().getAliasedName(), field.getName());
        } else {
            throw new RuntimeException("Expr of type: " + expr + " does not have an alias");
        }
    }

    static String columnNameWithIteration(final ColumnIdentifier node, final String iteratorVariable) {
        return String.format("%s[%s]", MinizincString.qualifiedName(node.getField()), iteratorVariable);
    }

    static String columnNameWithIteration(final ColumnIdentifier node) {
        return columnNameWithIteration(node, node.getTableName().toUpperCase(Locale.US) +  "__ITER");
    }

    static String groupColumnNameWithIteration(final String viewName, final ColumnIdentifier node) {
        return String.format("GROUP_TABLE__%s__%s%s[%s]", viewName.toUpperCase(Locale.getDefault()),
                                   node.fromGroupByWithDereference() ? node.getTableName() + "_" : "",
                                   node.getField().getName(), "GROUP__KEY");
    }

    static String literal(final Expr literal) {
        if (literal instanceof ColumnIdentifier) {
            return MinizincString.columnNameWithIteration((ColumnIdentifier) literal);
        } else if (literal instanceof MonoidLiteral) {
            return ((MonoidLiteral) literal).getValue().toString();
        }
        throw new RuntimeException("Unknown literal type " + literal);
    }


    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     *
     * @return Returns the MiniZinc name for the type. Strings type for MiniZinc is always an int
     */
    static String typeName(final IRColumn.FieldType type) {
        if (type.equals(IRColumn.FieldType.STRING)) {
            return "STRING_LITERALS";
        }
        return type.name().toLowerCase(Locale.US);
    }
}
