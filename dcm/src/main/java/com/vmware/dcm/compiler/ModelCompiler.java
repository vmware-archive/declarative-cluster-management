/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Record;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ModelCompiler {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);

    /**
     * Entry point to compile views into backend-specific code
     * @param tables a list of JOOQ Tables from the SQL Schema
     * @param views a list of strings, each of which is a view statement
     * @param backend an ISolverBackend instance.
     * @return A list of strings representing the output program that was compiled
     */
    @CanIgnoreReturnValue
    public List<String> compile(final List<Table<?>> tables, final List<SqlCreateConstraint> views,
                                final ISolverBackend backend) {
        final Map<String, IRTable> irTables = parseModel(tables);
        final IRContext irContext = new IRContext(irTables);

        LOG.debug("Compiling the following views\n{}", views);

        // Assert that all views are uniquely named
        final Set<String> names = new HashSet<>();
        views.forEach(view -> {
            final String name = view.getName().getSimple();
            if (names.contains(name)) {
                throw new ModelException("Duplicate name " + name);
            }
            names.add(name);
        });

        // Extract all the necessary views from the input code
        final Program<SqlCreateConstraint> sqlProgram = toSqlProgram(views);

        // Make sure the supplied views are only using the supported subset of SQL syntax
        sqlProgram.forEach((name, view) -> SyntaxChecking.apply(view));

        // Create IRTable entries for non-constraint views
        sqlProgram.nonConstraintViews()
                  .forEach((name, constraint) -> createIRTablesForNonConstraintViews(irContext, name,
                          constraint.getQuery()));

        // Convert from SQL to list comprehension syntax
        final Program<ListComprehension> irProgram = sqlProgram.map(view -> toListComprehension(irContext, view))
                .map((name, view) -> DesugarExists.apply(view));

        // Backend-specific code generation begins here.
        return backend.generateModelCode(irContext, irProgram);
    }

    private Program<SqlCreateConstraint> toSqlProgram(final List<SqlCreateConstraint> viewsWithChecks) {
        final Program<SqlCreateConstraint> program = new Program<>();
        viewsWithChecks.forEach(constraint -> {
                final String viewName = constraint.getName().toString();
                switch (constraint.getType()) {
                    case HARD_CONSTRAINT:
                        program.constraintViews().put(viewName, constraint);
                        break;
                    case OBJECTIVE:
                        program.objectiveFunctionViews().put(viewName, constraint);
                        break;
                    case INTERMEDIATE_VIEW:
                        program.nonConstraintViews().put(viewName, constraint);
                        break;
                    default:
                        throw new IllegalArgumentException(constraint.getType().name());
                }
            }
        );
        return program;
    }

    /*
     * A pass to create IRTable entries for non-constraint views. These views are used as intermediate
     * computations, so it is convenient in later stages of the compiler to have an IRTable entry for such views
     * to track relevant metadata (like column type information).
     */
    private void createIRTablesForNonConstraintViews(final IRContext irContext,
                                                     final String viewName, final SqlSelect query) {
        final FromExtractor fromParser = new FromExtractor(irContext);
        query.accept(fromParser);
        final Set<IRTable> tables = fromParser.getTables();
        final SqlNodeList selectList = query.getSelectList();
        createIRTablesFromSelectItems(irContext, selectList, tables, viewName);
    }

    /*
     * Given a list of select items, constructs IRColumns and IRTable entries for them.
     */
    private void createIRTablesFromSelectItems(final IRContext irContext, final SqlNodeList selectItems,
                                               final Set<IRTable> tablesReferencedInView, final String viewName) {
        final IRTable viewTable = new IRTable(null, viewName, viewName);
        selectItems.forEach(selectItem -> {
            final IRColumnsFromSelectItems visitor = new IRColumnsFromSelectItems(irContext, viewTable,
                                                                                  tablesReferencedInView);
            selectItem.accept(visitor);
        });
        irContext.addAliasedOrViewTable(viewTable);
    }

    private ListComprehension toListComprehension(final IRContext irContext, final SqlCreateConstraint view) {
        return TranslateViewToIR.apply(view.getQuery(), view.getConstraint(), irContext);
    }

    /**
     * Converts an SQL Table entry to a IR table, parsing and storing a reference to every field
     *  This includes Parsing foreign keys relationship between fields from different tables
     */
    private Map<String, IRTable> parseModel(final List<Table<?>> tables) {
        final Map<Table<?>, IRTable> tableIRTableMap = new HashMap<>();
        final Map<String, IRTable> irTableMap = new HashMap<>(tables.size());
        // parse the model for all the tables and fields
        for (final Table<?> table : tables) {
            final IRTable irTable = new IRTable(table);

            // parse all fields
            for (final Field<?> field : table.fields()) {
                final IRColumn irColumn = new IRColumn(irTable, field);
                irTable.addField(irColumn);
            }

            // After adding all the IRFields to the table, we parse the table UniqueKey
            // and link the correspondent IRFields as fields that compose the IRTable primary key
            final IRPrimaryKey pk = new IRPrimaryKey(irTable, table.getPrimaryKey());
            irTable.setPrimaryKey(pk);

            // add table reference to maps
            irTableMap.put(irTable.getName(), irTable);
            tableIRTableMap.put(table, irTable);
        }

        // parses foreign keys after initiating the tables
        // because for fks we need to setup relationships between different table fields
        for (final IRTable childTable : tableIRTableMap.values()) {
            // read table foreign keys, and init our map with the same size
            final List<? extends ForeignKey<? extends Record, ?>> foreignKeys = childTable.getTable().getReferences();
            for (final ForeignKey<? extends Record, ?> fk : foreignKeys) {
                // table referenced by the foreign key
                final IRTable parentTable = tableIRTableMap.get(fk.getKey().getTable());

                // TODO: ideally, we should recurse and find all tables at the expense of bringing in
                //       more data than we need at runtime
                // https://github.com/vmware/declarative-cluster-management/issues/108
                if (parentTable == null) {
                    continue;
                }

                // build foreign key based on the fk fields
                final IRForeignKey irForeignKey = new IRForeignKey(childTable, parentTable, fk);

                // adds new foreign key to the table
                childTable.addForeignKey(irForeignKey);
            }
        }
        return irTableMap;
    }
}