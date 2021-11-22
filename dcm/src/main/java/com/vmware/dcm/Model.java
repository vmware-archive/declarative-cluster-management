/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Query;
import com.google.common.annotations.VisibleForTesting;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.compiler.ModelCompiler;
import com.vmware.dcm.generated.parser.DcmSqlParserImpl;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 *   - solve() to solve the current model based on the current modelFile and dataFile
 */
public class Model {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);
    private final DSLContext dbCtx;
    private final List<Table<? extends Record>> jooqTables;
    private final ISolverBackend backend;
    private final List<String> generatedCode;
    private final IRContext irContext;

    private Model(final DSLContext dbCtx, final ISolverBackend backend, final List<Table<?>> tables,
                  final List<String> constraints) {
        this(dbCtx, backend, tables, constraints, dbCtx);
    }

    private Model(final DSLContext dbCtx, final ISolverBackend backend, final List<Table<?>> tables,
                  final List<String> constraints, final DSLContext dbCtxForMeta) {
        if (tables.isEmpty()) {
            throw new ModelException("Model does not contain any constraints or " +
                                     "constraints do not refer to any tables");
        }
        this.dbCtx = dbCtx;
        // for pretty-print query - useful for debugging
        this.dbCtx.settings().withRenderFormatted(true);
        this.backend = backend;
        final List<SqlCreateConstraint> constraintViews = constraints.stream().map(
                constraint -> {
                    try {
                        final SqlParser.Config config = SqlParser.config()
                                .withParserFactory(DcmSqlParserImpl.FACTORY)
                                .withConformance(SqlConformanceEnum.LENIENT);
                        final SqlParser parser = SqlParser.create(constraint, config);
                        return (SqlCreateConstraint) parser.parseStmt();
                    } catch (final SqlParseException e) {
                        LOG.error("Could not parse view: {}", constraint, e);
                        throw new ModelException(e);
                    }
                }
        ).collect(Collectors.toList());

        /*
         * Identify additional views to create in the database for group bys. These views will be added to
         * the DB, and we augment the list of tables to pass to the compiler with these views.
         *
         * We retain the use of the Presto parser here given that this codepath is only used for the MiniZinc solver.
         */
        final Set<String> createdViewNames = new HashSet<>();
        if (backend.needsGroupTables()) {
            final List<CreateView> groupByViewsToCreate = constraintViews.stream().map(view -> {
                final com.facebook.presto.sql.parser.SqlParser parser = new com.facebook.presto.sql.parser.SqlParser();
                final String s = view.getQuery().toString().replace("`", "");
                final Query statement = (Query) parser.createStatement(s, ParsingOptions.builder().build());
                final ExtractGroupTable groupTable = new ExtractGroupTable();
                return groupTable.process(view.getName().toString(), statement);
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
        for (final Table<?> table: dbCtxForMeta.meta().getTables()) {
            if (createdViewNames.contains(table.getName().toUpperCase(Locale.getDefault()))) {
                augmentedTableList.add(table);
            }
        }

        // parse model from SQL tables
        jooqTables = augmentedTableList;
        final ModelCompiler compiler = new ModelCompiler();
        generatedCode = compiler.compile(augmentedTableList, constraintViews, backend);
        this.irContext = compiler.getIrContext();
    }

    public IRContext getIrContext() {
        return irContext;
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

    public static Model build(final DSLContext dslContext, final ISolverBackend solverBackend,
                              final List<String> constraints, final DSLContext dslContextForMeta) {
        final List<Table<?>> tables = getTablesFromContext(dslContextForMeta, constraints);
        return new Model(dslContext, solverBackend, tables, constraints, dslContextForMeta);
    }

    /**
     * Finds all the tables that are referenced by the supplied constraints
     *
     * @return list of all the tables referenced by constraints
     */
    private static List<Table<?>> getTablesFromContext(final DSLContext dslContext, final List<String> constraints) {
        final Set<String> accessedTableNames = new HashSet<>();
        constraints.forEach(constraint -> {
            final SqlParser.Config config = SqlParser.config().withParserFactory(DcmSqlParserImpl.FACTORY)
                                                              .withConformance(SqlConformanceEnum.LENIENT);
            final SqlParser parser = SqlParser.create(constraint, config);
            try {
                final SqlCreateConstraint ddl = (SqlCreateConstraint) parser.parseStmt();
                final ExtractAccessedTables visitor = new ExtractAccessedTables(accessedTableNames);
                visitor.visit(ddl);
            } catch (final SqlParseException e) {
                throw new ModelException("Could not parse constraint:\n" + constraint, e);
            }
        });

        final Meta dslMeta = dslContext.meta();
        final List<Table<?>> tables = new ArrayList<>();
        for (final Table<?> t : dslMeta.getTables()) {
            // Only access the tables that are referenced by the constraints.
            if (accessedTableNames.contains(t.getName().toUpperCase())) {
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
     * Solves the current model and returns the records for the specified set of tables. If any of these
     * tables have variable columns, they will reflect the changes made by the solver.
     *
     * @param tables a set of table names
     * @return A map where keys correspond to the supplied "tables" parameter, and the values are Result objects
     *         representing rows of the corresponding tables, with modifications made by the solver
     */
    public Map<String, Result<? extends Record>> solve(final Set<String> tables) throws ModelException {
        return solve(tables, this::defaultFetcher);
    }

    /**
     * Solves the current model and returns the records for the specified set of tables. If any of these
     * tables have variable columns, they will reflect the changes made by the solver.
     *
     * @param tables a set of table names
     * @param fetcher a function that given a JOOQ Table, fetches the corresponding result set as a JOOQ Result.
     * @return A map where keys correspond to the supplied "tables" parameter, and the values are Result objects
     *         representing rows of the corresponding tables, with modifications made by the solver
     */
    public Map<String, Result<? extends Record>> solve(final Set<String> tables,
                                                       final Function<Table<?>, Result<? extends Record>> fetcher)
                                                                                                throws ModelException {
        final Set<String> tablesInUpperCase = tables.stream().map(String::toUpperCase).collect(Collectors.toSet());
        // run the solver and get a result set per table
        LOG.info("Running the solver");
        final long start = System.nanoTime();
        final Map<String, Result<? extends Record>> inputRecords = fetchRecords(fetcher);
        final Map<String, Result<? extends Record>> recordsPerTable = backend.runSolver(inputRecords);
        LOG.info("Solver has run successfully in {}ns. Processing records.", System.nanoTime() - start);
        final Map<String, Result<? extends Record>> recordsToReturn = new HashMap<>();
        for (final Map.Entry<String, Result<? extends Record>> entry: recordsPerTable.entrySet()) {
            final String entryName = entry.getKey().toUpperCase();
            if (tablesInUpperCase.contains(entryName)) {
                recordsToReturn.put(entryName, entry.getValue());
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
    public Result<? extends Record> solve(final String tableName)
            throws ModelException {
        return solve(Set.of(tableName)).get(tableName);
    }

    /**
     * Solves the current model and returns the records for the specified table. If the
     * table has variable columns, the returned result will reflect the changes made by the solver.
     *
     * @param tableName a table name
     * @param fetcher a function that given a JOOQ Table, fetches the corresponding result set as a JOOQ Result.
     * @return A Result object representing rows of the corresponding tables, with modifications made by the solver
     */
    public Result<? extends Record> solve(final String tableName,
                                          final Function<Table<?>, Result<? extends Record>> fetcher)
            throws ModelException {
        return solve(Set.of(tableName), fetcher).get(tableName);
    }

    /**
     * Scans tables to update the data associated with the model
     */
    private Map<String, Result<? extends Record>> fetchRecords(
            final Function<Table<?>, Result<? extends Record>> fetcher) {
        final Map<String, Result<? extends Record>> records = new LinkedHashMap<>();
        final long updateData = System.nanoTime();
        for (final Table<? extends Record> table: jooqTables) {
            final long start = System.nanoTime();
            final Result<? extends Record> recentData = fetcher.apply(table);
            Objects.requireNonNull(recentData, "Table Result<?> was null");
            final long select = System.nanoTime();
            records.put(table.getName(), recentData);
            final long updateValues = System.nanoTime();
            LOG.info("updateDataFields for table {} took {} ns to fetch {} rows from DB, " +
                            "and {} ns to reflect in IRTables",
                     table.getName(), (select - start), recentData.size(), (System.nanoTime() - updateValues));
        }
        LOG.info("compiler.updateData() took {}ns to complete", (System.nanoTime() - updateData));
        return records;
    }

    Result<? extends Record> defaultFetcher(final Table<?> table) {
        return dbCtx.selectFrom(table.getUnqualifiedName()).fetch();
    }


    /*
     * Used for white box tests
     */
    @VisibleForTesting
    List<String> compilationOutput() {
        return generatedCode;
    }
}