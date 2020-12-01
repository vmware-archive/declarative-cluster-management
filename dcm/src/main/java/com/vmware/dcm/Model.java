/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.CreateView;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.compiler.ModelCompiler;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Meta;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Used to synthesize a model from a set of SQL tables and constraints.
 *
 * The public API for Model involves three narrow interfaces:
 *
 *   - buildModel() to create Model instances based on a supplied JOOQ DSLContext.
 *   - updateData() to extract the input data for the Model that we can then feed to solvers.
 *   - solveModel() to solve the current model based on the current modelFile and dataFile
 */
public class Model {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);
    private final DSLContext dbCtx;
    private final Map<Table<? extends Record>, IRTable> jooqTableToIRTable;
    private final Map<String, IRTable> irTables;
    private final ModelCompiler compiler;
    private final IRContext irContext;
    private final ISolverBackend backend;

    private Model(final DSLContext dbCtx, final ISolverBackend backend, final List<Table<?>> tables,
                  final List<String> constraints) {
        if (tables.isEmpty()) {
            throw new ModelException("Model does not contain any constraints or " +
                                     "constraints do not refer to any tables");
        }
        this.dbCtx = dbCtx;
        // for pretty-print query - useful for debugging
        this.dbCtx.settings().withRenderFormatted(true);
        this.backend = backend;
        final List<ViewsWithChecks> constraintViews = constraints.stream().map(
                constraint -> {
                    try {
                        return ViewsWithChecks.fromString(constraint);
                    } catch (final ParsingException e) {
                        LOG.error("Could not parse view: {}", constraint, e);
                        throw e;
                    }
                }
        ).collect(Collectors.toList());

        /*
         * Identify additional views to create in the database for group bys. These views will be added to
         * the DB, and we augment the list of tables to pass to the compiler with these views.
         */
        final Set<String> createdViewNames = new HashSet<>();
        if (backend.needsGroupTables()) {
            final List<CreateView> groupByViewsToCreate = constraintViews.stream().map(view -> {
                final ExtractGroupTable groupTable = new ExtractGroupTable();
                return groupTable.process(view.getCreateView());
            }).filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            groupByViewsToCreate.forEach(view -> {
                final String s = SqlFormatter.formatSql(view, Optional.empty());
                dbCtx.execute(s);
            });
            final Set<String> viewsToCreate = groupByViewsToCreate.stream().map(view -> view.getName().toString()
                                                                            .toUpperCase(Locale.getDefault()))
                                                                            .collect(Collectors.toSet());
            createdViewNames.addAll(viewsToCreate);
        }
        final List<Table<?>> augmentedTableList = new ArrayList<>(tables);
        // dbCtx.meta().getTables(<name>) is buggy https://github.com/jOOQ/jOOQ/issues/7686,
        // so we're going to scan all tables and pick the ones whose names match that of the views we created.
        for (final Table<?> table: dbCtx.meta().getTables()) {
            if (createdViewNames.contains(table.getName().toUpperCase(Locale.getDefault()))) {
                augmentedTableList.add(table);
            }
        }

        // parse model from SQL tables
        jooqTableToIRTable = new HashMap<>(augmentedTableList.size());
        irTables = new HashMap<>(augmentedTableList.size());
        parseModel(augmentedTableList);
        irContext = new IRContext(irTables);
        compiler = new ModelCompiler(irContext);
        compiler.compile(constraintViews, backend);
    }

    /**
     * Builds a model out of dslContext, using the OR-Tools solver as a backend.
     *
     * @param dslContext JOOQ DSLContext to use to find tables representing the model.
     * @param constraints The hard and soft constraints, with one view per string
     * @return An initialized Model instance
     */
    @SuppressWarnings({"WeakerAccess", "reason=Public API"})
    public static Model build(final DSLContext dslContext, final List<String> constraints) {
        final List<Table<?>> tables = getTablesFromContext(dslContext, constraints);
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder().build();
        return new Model(dslContext, orToolsSolver, tables, constraints);
    }

    /**
     * Builds a model out of dslContext with the supplied solver backend.
     *
     * @param dslContext JOOQ DSLContext to use to find tables representing the model.
     * @param solverBackend A solver implementation. See the ISolverBackend class.
     * @param constraints The hard and soft constraints, with one view per string
     * @return An initialized Model instance
     */
    @SuppressWarnings({"WeakerAccess", "reason=Public API"})
    public static Model build(final DSLContext dslContext, final ISolverBackend solverBackend,
                              final List<String> constraints) {
        final List<Table<?>> tables = getTablesFromContext(dslContext, constraints);
        return new Model(dslContext, solverBackend, tables, constraints);
    }

    /**
     * Finds all the tables that are referenced by the supplied constraints
     *
     * @return list of all the tables referenced by constraints
     */
    private static List<Table<?>> getTablesFromContext(final DSLContext dslContext, final List<String> constraints) {
        final Set<String> accessedTableNames = new HashSet<>();
        constraints.forEach(constraint -> {
            final ViewsWithChecks viewsWithChecks = ViewsWithChecks.fromString(constraint);
            final ExtractAccessedTables visitor = new ExtractAccessedTables(accessedTableNames);
            visitor.process(viewsWithChecks.getCreateView());
            viewsWithChecks.getCheckExpression().ifPresent(visitor::process);
        });

        final Meta dslMeta = dslContext.meta();
        final List<Table<?>> tables = new ArrayList<>();
        for (final Table<?> t : dslMeta.getTables()) {
            // Only access the tables that are referenced by the constraints.
            if (accessedTableNames.contains(t.getName())) {
                tables.add(t);

                // Also add tables referenced by foreign keys.
                t.getReferences().forEach(
                    fk -> tables.add(fk.getKey().getTable())
                );
            }
        }
        return tables;
    }

    /**
     * Updates the input data for a model by getting the latest data from the required tables.
     */
    public synchronized void updateData() {
        updateDataFields(this::defaultFetcher);
    }

    /**
     * Updates the input data for a model by using the supplied callback to retrieve the
     * result-set for each table or view.
     *
     * @param fetcher a function that given a JOOQ Table, fetches the corresponding result set as a JOOQ Result.
     */
    public synchronized void updateData(final Function<Table<?>, Result<? extends Record>> fetcher) {
        updateDataFields(fetcher);
    }

    /**
     * Solves the current model and returns the records for the specified set of tables. If any of these
     * tables have variable columns, they will reflect the changes made by the solver.
     *
     * @param tables a set of table names
     * @return A map where keys correspond to the supplied "tables" parameter, and the values are Result objects
     *         representing rows of the corresponding tables, with modifications made by the solver
     */
    public synchronized Map<String, Result<? extends Record>> solve(final Set<String> tables)
            throws ModelException {
        // run the solver and get a result set per table
        LOG.info("Running the solver");
        final long start = System.nanoTime();
        final Map<IRTable, Result<? extends Record>> recordsPerTable = backend.runSolver(dbCtx, irTables);
        LOG.info("Solver has run successfully in {}ns. Processing records.", System.nanoTime() - start);
        final Map<String, Result<? extends Record>> recordsToReturn = new HashMap<>();
        for (final Map.Entry<IRTable, Result<? extends Record>> entry: recordsPerTable.entrySet()) {
            if (tables.contains(entry.getKey().getName())) {
                recordsToReturn.put(entry.getKey().getName(), entry.getValue());
            }
        }
        return recordsToReturn;
    }


    /**
     * Solves the current model and returns the records for the specified table. If the
     * table has variable columns, the returned result will reflect the changes made by the solver.
     *
     * @param tableName a table name
     * @return A Result object representing rows of the corresponding tables, with modifications made by the solver
     */
    public synchronized Result<? extends Record> solve(final String tableName)
            throws ModelException {
        return solve(Set.of(tableName)).get(tableName);
    }

    /**
     * Converts an SQL Table entry to a IR table, parsing and storing a reference to every field
     *  This includes Parsing foreign keys relationship between fields from different tables
     */
    private void parseModel(final List<Table<?>> tables) {
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
            jooqTableToIRTable.put(table, irTable);
            irTables.put(irTable.getName(), irTable);
        }

        // parses foreign keys after initiating the tables
        // because for fks we need to setup relationships between different table fields
        for (final IRTable childTable : jooqTableToIRTable.values()) {
            // read table foreign keys, and init our map with the same size
            final List<? extends ForeignKey<? extends Record, ?>> foreignKeys = childTable.getTable().getReferences();
            for (final ForeignKey<? extends Record, ?> fk : foreignKeys) {
                // table referenced by the foreign key
                final IRTable parentTable = jooqTableToIRTable.get(fk.getKey().getTable());

                // build foreign key based on the fk fields
                final IRForeignKey irForeignKey = new IRForeignKey(childTable, parentTable, fk);

                // adds new foreign key to the table
                childTable.addForeignKey(irForeignKey);
            }
        }
    }

    /**
     * Scans tables to update the data associated with the model
     */
    private void updateDataFields(final Function<Table<?>, Result<? extends Record>> fetcher) {
        final long updateData = System.nanoTime();
        for (final Map.Entry<Table<? extends Record>, IRTable> entry : jooqTableToIRTable.entrySet()) {
            final Table<? extends Record> table = entry.getKey();
            final IRTable irTable = entry.getValue();
            final long start = System.nanoTime();
            final Result<? extends Record> recentData = fetcher.apply(table);
            Objects.requireNonNull(recentData, "Table Result<?> was null");
            final long select = System.nanoTime();
            irTable.updateValues(recentData);
            final long updateValues = System.nanoTime();
            LOG.info("updateDataFields for table {} took {} ns to fetch {} rows from DB, " +
                            "and {} ns to reflect in IRTables",
                     table.getName(), (select - start), recentData.size(), (System.nanoTime() - updateValues));
        }
        compiler.updateData(irContext, backend);
        LOG.info("compiler.updateData() took {}ns to complete", (System.nanoTime() - updateData));
    }

    Result<? extends Record> defaultFetcher(final Table<?> table) {
        return dbCtx.selectFrom(table).fetch();
    }
}