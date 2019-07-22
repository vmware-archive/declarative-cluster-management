/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import java.util.List;
import java.util.Map;

public class OrToolsSolver implements ISolverBackend {
    private Map<String, MonoidComprehension> nonConstraintViews;
    private Map<String, MonoidComprehension> constraintViews;
    private Map<String, MonoidComprehension> objectiveFunctions;

    @Override
    public Map<IRTable, Result<? extends Record>> runSolver(final DSLContext dbCtx,
                                                            final Map<String, IRTable> irTables) {
        return null;
    }

    @Override
    public List<String> generateModelCode(final IRContext context,
                                          final Map<String, MonoidComprehension> nonConstraintViews,
                                          final Map<String, MonoidComprehension> constraintViews,
                                          final Map<String, MonoidComprehension> objectiveFunctions) {
        this.nonConstraintViews = nonConstraintViews;
        this.constraintViews = constraintViews;
        this.objectiveFunctions = objectiveFunctions;

        // array declarations
        // Create the model.
//        final CpModel model = new CpModel();

        // Tables
//        for (final IRTable table: context.getTables()) {
//            if (table.isViewTable() || table.isAliasedTable()) {
//                continue;
//            }

            // Fields
//            for (final Map.Entry<String, IRColumn> fieldEntrySet: table.getIRColumns().entrySet()) {
//                final String fieldName = fieldEntrySet.getKey();
//                final IRColumn field = fieldEntrySet.getValue();
//                ret.add(String.format("%% %s", fieldName));
//                ret.add(String.format("array[1..%s] %s %s : %s;",
//                        MinizincString.tableNumRowsName(table),
//                        field.isControllable() ? "of var" : "of",
//                        MinizincString.typeName(field.getType()),
//                        MinizincString.qualifiedName(field)));
//                if (field.isControllable()) {
//                    intVarNoBounds(model, "x" + 1);
//                }
//            }

//            table.getPrimaryKey()
//                    .ifPresent(pk -> ret.add(getPrimaryKeyDeclaration(pk)));
//
//            table.getForeignKeys()
//                    .forEach(fk -> ret.add(getForeignKeyDeclaration(fk)));
//
//            ret.add(getOutputStatementForTable(table));
//        }

        return null;
    }

    @Override
    public List<String> generateDataCode(final IRContext context) {
        // array declarations
        // Create the model.
        final CpModel model = new CpModel();

        // Tables
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }

            // Fields
            int i = 0;
            for (final Map.Entry<String, IRColumn> fieldEntrySet : table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();
                if (field.isControllable()) {
                    intVarNoBounds(model, String.format("fieldName-%d", i));
                }
            }
        }

        return null;
    }

    private IntVar intVarNoBounds(final CpModel model, final String name) {
        return model.newIntVar(0, Integer.MAX_VALUE - 1, name);
    }
}
