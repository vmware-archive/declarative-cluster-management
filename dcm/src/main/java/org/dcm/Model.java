/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateView;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
 
import org.dcm.backend.ISolverBackend;
import org.dcm.backend.MinizincSolver;
import org.dcm.compiler.ModelCompiler;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;

import org.jooq.Constraint;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Meta;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.RowN;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.values;


/**
 * Used synthesize a MiniZinc model based on a set of SQL tables.
 *
 * The public API for Model involves two narrow interfaces:
 *
 *   - buildModel() to create Model instances based on a supplied JOOQ DSLContext.
 *   - updateData() to extract the input data for the Model that we can then feed to solvers.
 *   - solveModel() to solve the current model based on the current modelFile and dataFile
 */
public class Model {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);
    private static final String CURRENT_SCHEMA = "CURR";
    private static final SqlParser PARSER = new SqlParser();
    private final ParsingOptions options = new ParsingOptions();
    private final DSLContext dbCtx;
    private final Map<Table<? extends Record>, IRTable> jooqTableToIRTable;
    private final Map<String, IRTable> irTables;
    private final Multimap<Table, Constraint> jooqTableConstraintMap;
    private final ModelCompiler compiler;
    private IRContext irContext;
    private final ISolverBackend backend;
    private final Map<String, Integer> tableIDMap = new HashMap<>();
    private final DDlogAPI api = new DDlogAPI(1, null, false);

    private Model(final DSLContext dbCtx, final List<Table<?>> tables, final List<String> views,
                  final File modelFile, final File dataFile, final Conf conf) {
        api.record_commands("replay.dat", false);
        this.dbCtx = dbCtx;
        // for pretty-print query - useful for debugging
        this.dbCtx.settings().withRenderFormatted(true);
        this.backend = new MinizincSolver(modelFile, dataFile, conf);
        final List<CreateView> viewsInPolicy = views.stream().map(
                view -> (CreateView) PARSER.createStatement(view, options)
        ).collect(Collectors.toList());

        /*
         * Identify additional views to create in the database for group bys. These views will be added to
         * the DB, and we augment the list of tables to pass to the compiler with these views.
         */
        final List<CreateView> groupByViewsToCreate = viewsInPolicy.stream().map(view -> {
            final ExtractGroupTable groupTable = new ExtractGroupTable();
            return groupTable.process(view);
        }).filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());
        groupByViewsToCreate.forEach(view -> {
            final String s = SqlFormatter.formatSql(view, Optional.empty());
            dbCtx.execute(s);
        });
        final Set<String> createdViewNames = groupByViewsToCreate.stream().map(view -> view.getName()
                                                                                           .toString()
                                                                                     .toUpperCase(Locale.getDefault()))
                                                                          .collect(Collectors.toSet());
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
        jooqTableConstraintMap = HashMultimap.create();
        irTables = new HashMap<>(augmentedTableList.size());
        parseModel(augmentedTableList);
        for (final Map.Entry<Table<? extends Record>, IRTable> entry : jooqTableToIRTable.entrySet()) {
            // TODO: uncomment if removal of pk constraints is needed
            // final UniqueKey pk = table.getPrimaryKey();
            // constraints.put(table, pk.constraint());
            // dbCtx.alterTable(table).drop(pk.constraint()).execute();

            // remove fk constraints
            final Table<? extends Record> table = entry.getKey();
            for (final ForeignKey fk : table.getReferences()) {
                jooqTableConstraintMap.put(table, fk.constraint());
            }
        }
        irContext = new IRContext(irTables);
        compiler = new ModelCompiler(irContext);
        compiler.compile(viewsInPolicy, backend);
    }


    /**
     * Builds a Minizinc model out of dslContext
     *
     * @param dslContext JOOQ DSLContext from which Weave finds the tables for which a Minizinc model will be generated.
     * @param modelFile A file into which the Minizinc model (.mnz) will be written before this method returns
     * @param dataFile A file into which the data (.dzn) for the Minizinc model will be written at runtime, when
     *                 updateData() is invoked
     * @return An initialized Model instance with a populated modelFile
     */
    @SuppressWarnings({"WeakerAccess", "reason=Public API"})
    public static synchronized Model buildModel(final DSLContext dslContext, final List<String> views,
                                                final File modelFile, final File dataFile) {
        final List<Table<?>> tables = getTablesFromContext(dslContext);
        return buildModel(dslContext, tables, views, modelFile, dataFile);
    }

    /**
     * Builds a Minizinc model out of dslContext
     *
     * @param dslContext JOOQ DSLContext from which Weave finds the tables for which a Minizinc model will be generated.
     * @param modelFile A file into which the Minizinc model (.mnz) will be written before this method returns
     * @param dataFile A file into which the data (.dzn) for the Minizinc model will be written at runtime, when
     *                 updateData() is invoked
     * @param conf configuration object
     * @return An initialized Model instance with a populated modelFile
     */
    @SuppressWarnings({"WeakerAccess", "reason=Public API"})
    public static synchronized Model buildModel(final DSLContext dslContext, final List<String> views,
                                                final File modelFile, final File dataFile, final Conf conf) {
        final List<Table<?>> tables = getTablesFromContext(dslContext);
        return buildModel(dslContext, tables, views, modelFile, dataFile, conf);
    }

    /**
     * Builds a Minizinc model out of dslContext and a list of tables.
     *
     * @param dslContext JOOQ DSLContext from which Weave finds the tables for which a Minizinc model will be generated.
     * @param tables A set of tables for which the Minzinc model will be generated
     * @param modelFile A file into which the Minizinc model (.mnz) will be written before this method returns
     * @param dataFile A file into which the data (.dzn) for the Minizinc model will be written at runtime, when
     *                 updateData() is invoked
     * @return An initialized Model instance with a populated modelFile
     */
    @SuppressWarnings({"WeakerAccess", "reason=Public API"})
    public static synchronized Model buildModel(final DSLContext dslContext, final List<Table<?>> tables,
                                                final List<String> views, final File modelFile,
                                                final File dataFile) {
        return buildModel(dslContext, tables, views, modelFile, dataFile, new Conf());
    }

    /**
     * Builds a Minizinc model out of dslContext and a list of tables.
     *
     * @param dslContext JOOQ DSLContext from which Weave finds the tables for which a Minizinc model will be generated.
     * @param tables A set of tables for which the Minzinc model will be generated
     * @param modelFile A file into which the Minizinc model (.mnz) will be written before this method returns
     * @param dataFile A file into which the data (.dzn) for the Minizinc model will be written at runtime, when
     *                 updateData() is invoked
     * @param conf configuration object
     * @return An initialized Model instance with a populated modelFile
     */
    @SuppressWarnings({"WeakerAccess", "reason=Public API"})
    public static synchronized Model buildModel(final DSLContext dslContext, final List<Table<?>> tables,
                                                final List<String> views, final File modelFile,
                                                final File dataFile, final Conf conf) {
        return new Model(dslContext, tables, views, modelFile, dataFile, conf);
    }

    /**
     * Finds all the tables that are on the CURRENT_SCHEMA on the given DSLContext
     *
     * @return list of all the tables on the CURRENT_SCHEMA
     */
    private static List<Table<?>> getTablesFromContext(final DSLContext dslContext) {
        final Meta dslMeta = dslContext.meta();
        final List<Table<?>> tables = new ArrayList<>();
        for (final Table<?> t : dslMeta.getTables()) {
            // skip if table not on current schema
            if (!t.getSchema().getName().equals(CURRENT_SCHEMA)) {
                continue;
            }
            tables.add(t);
        }
        return tables;
    }

    /**
     * Updates the data file within a model by getting the latest data from the tables
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void updateData() {
        updateDataFields();
    }

    DDlogRecord toDDlogRecord(final String className, final Record record) {
        final List<DDlogRecord> records = new ArrayList<>();
        for (final Field<?> field: record.fields()) {
                final Class<?> cls = field.getType();
                if (cls.getName().equals("java.lang.Boolean")) {
                    records.add(new DDlogRecord((Boolean) field.getValue(record)));
                } else if (cls.getName().equals("java.lang.Integer") || cls.getName().equals("int")) {
                    records.add(new DDlogRecord((Integer) field.getValue(record)));
                } else if (cls.getName().equals("java.lang.Long") || cls.getName().equals("long")) {
                    records.add(new DDlogRecord((Long) field.getValue(record)));
                } else if (cls.getName().equals("java.lang.String")) {
                    records.add(new DDlogRecord(String.valueOf(field.getValue(record))));
                } else {
                    throw new RuntimeException("unexpected datatype: "  + cls.getName());
                }
        }
        DDlogRecord[] recordsArray = new DDlogRecord[records.size()];
        recordsArray = records.toArray(recordsArray);
        return DDlogRecord.makeStruct(className, recordsArray);
    }

    synchronized void updateDataWithDDLog() {
        final List<DDlogCommand> commands = new ArrayList<>();
        for (final Map.Entry<Table<? extends Record>, IRTable> entry : jooqTableToIRTable.entrySet()) {
            final Table<? extends Record> table = entry.getKey();
            final Result<? extends Record> recentData = dbCtx.selectFrom(table).fetch();
            for (final Record record: recentData) {
                final DDlogRecord ddlogRecord  = toDDlogRecord(table.getName(), record);
                final String relation = ddlogRecord.getStructName();
                int id;
                if (tableIDMap.containsKey(relation)) {
                    id = tableIDMap.get(relation);
                }
                else  {
                    id = api.getTableId(relation);
                    tableIDMap.put(relation, id);
                }
                commands.add(new DDlogCommand(DDlogCommand.Kind.Insert, id, ddlogRecord));
            }
        }

        final DDlogCommand [] ca = commands.toArray(new DDlogCommand[commands.size()]);
        checkExitCode(api.start());
        checkExitCode(api.applyUpdates(ca));
        checkExitCode(api.commit_dump_changes(this::commit));

        updateDataFields();
    }

    void checkExitCode(final int exitCode) {
        if (exitCode < 0) {
            throw new RuntimeException("Error executing " + exitCode);
        }
    }

    synchronized void commit(final DDlogCommand command) {
        final DDlogRecord record = command.value;
        final String dataType = record.getStructName();

        if (irTables.containsKey(dataType)) {
            final StringBuilder stringBuilder = new StringBuilder();
            final IRTable irTable = irTables.get(dataType);
            final Table<? extends Record> table = irTable.getTable();
            final Field[] fields = table.fields();
            if (command.kind == DDlogCommand.Kind.Insert) {
                stringBuilder.append("insert into " + dataType + " values ( \n");
                int counter = 0;
                for (final Field field : fields) {
                    final Class fieldClass = field.getType();
                    final DDlogRecord item = record.getStructField(counter);

                    if (fieldClass.getName().equals("java.lang.String")) {
                        stringBuilder.append("'" + item.getString() + "'");
                    } else if (fieldClass.getName().equals("java.lang.Long")) {
                        stringBuilder.append(item.getLong());
                    } else if (fieldClass.getName().equals("java.lang.Integer")) {
                        stringBuilder.append(item.getU128());
                    } else if (fieldClass.getName().equals("java.lang.Boolean")) {
                        stringBuilder.append(item.getBoolean());
                    }
                    if (counter < fields.length - 1) {
                        stringBuilder.append(", ");
                    }
                    counter = counter + 1;
                }
            } else if (command.kind == DDlogCommand.Kind.DeleteVal) {
                stringBuilder.append("delete from " + dataType + " where ( \n");
                int counter = 0;
                for (final Field field : fields) {
                    final Class fieldClass = field.getType();
                    final DDlogRecord item = record.getStructField(counter);
                    if (fieldClass.getName().equals("java.lang.String")) {
                        stringBuilder.append(field.getName() + " = '" + item.getString() + "'");
                    } else if (fieldClass.getName().equals("java.lang.Long")) {
                        stringBuilder.append(field.getName() + " = " + item.getLong());
                    } else if (fieldClass.getName().equals("java.lang.Integer")) {
                        stringBuilder.append(field.getName() + " = " + item.getU128());
                    } else if (fieldClass.getName().equals("java.lang.Boolean")) {
                        stringBuilder.append(field.getName() + " = " + item.getBoolean());
                    }
                    if (counter < fields.length - 1) {
                        stringBuilder.append(" and ");
                    }
                    counter = counter + 1;
                }
            }
            stringBuilder.append("\n)");
            final String query = stringBuilder.toString();
            System.out.println(query);
            dbCtx.execute(query);
        }
    }

    /**
     * Solves the current model by running the current modelFile and dataFile against MiniZinc
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void solveModel() throws ModelException {
        // run the solver and get a result set per table
        LOG.info("Running the solver");
        final long start = System.nanoTime();
        final Map<IRTable, Result<? extends Record>> recordsPerTable = backend.runSolver(dbCtx, irTables);
        LOG.info("Solver has run successfully in {}ns. Processing records.", System.nanoTime() - start);
        // write changes to each table
        updateTables(recordsPerTable);
    }

    /**
     * Solves the current model by running the current modelFile and dataFile against MiniZinc
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized Map<String, Result<? extends Record>> solveModelWithoutTableUpdates(final Set<String> tables)
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
     * Updates the database tables based on the output from the MiniZinc model.
     *
     * XXX: This method is currently a performance bottleneck.
     */
    @SuppressWarnings("unchecked")
    private void updateTables(final Map<IRTable, Result<? extends Record>> recordsPerTable) {
        LOG.info("Removing constraints");

        // We temporarily remove constraints so we avoid SQL errors related to ForeignKey errors
        removeConstraints(jooqTableConstraintMap);

        for (final Map.Entry<IRTable, Result<? extends Record>> tableEntry : recordsPerTable.entrySet()) {
            final IRTable irTable = tableEntry.getKey();
            LOG.info("Updating rows for table: {}", irTable.getName());

            // if a table has no variables, there will be no new values from MiniZinc output to write
            // hence we just skip that value
            if (irTable.getVars().isEmpty()) {
                continue;
            }

            final Table table = irTable.getTable();
            final Result<? extends Record> records = tableEntry.getValue();

            // transform the result into a temporary VALUES table
            // https://www.jooq.org/doc/latest/manual/sql-building/table-expressions/values/
            final RowN[] valuesRows = records.stream().map(Record::valuesRow).toArray(RowN[]::new);

            // Rundown of the different query parts:
            // - Current Table:
            //      dbCtx.selectFrom(table)
            // - Temporary VALUES table built from the MiniZinc output
            //      dbCtx.selectFrom(values(valuesRows).as(table, table.fields()))

            // Delete rows on current table that are not in the MiniZinc output
            // computed as: Current Table MINUS MiniZinc output table
            final Result<? extends Record> rowsToDelete = dbCtx.selectFrom(table)
                    .except(dbCtx.selectFrom(values(valuesRows).as(table, table.fields())))
                    .fetchInto(table);

            if (rowsToDelete.size() == 0) {
                continue;
            }

            // jOOQ's `batchDelete` won't work here because it needs a list of `UpdatableRecord`s,
            // but `UpdatableRecord` only exists for tables with a primary key.
            // Since we are not assuming that, we execute an row-by-row delete.
            dbCtx.transaction(configuration -> {
                final DSLContext txCtx = DSL.using(configuration);
                for (final Record recordToDelete : rowsToDelete) {
                    txCtx.delete(table).where(row(table.fields()).eq(recordToDelete.intoArray())).execute();
                }

            });

            // Insert new rows from MiniZinc output
            // computed as: MiniZinc output table MINUS Current Table
            dbCtx.insertInto(table)
                    .select(dbCtx.selectFrom(values(valuesRows).as(table, table.fields()))
                                 .except(dbCtx.selectFrom(table)))
                    .execute();
        }

        // FIXME: If any exception is thrown while performing the operations in the tables we might not be able to
        // restore the constraints here again. Even if we add a try-finally block, there might be the case when the
        // table was left at an inconsistent state and then when we are unable to restore constraints
        // TODO: Observe the different exceptions that might get thrown in this method and handle them overtime

        // After updating the DB we restore the constraints that we removed before
        restoreConstraints(jooqTableConstraintMap);

        LOG.info("Wrote output to the given DB context! You can read changes now.");
    }

    /**
     * Restore previously removed constraints
     */
    private void restoreConstraints(final Multimap<Table, Constraint> constraints) {
        for (final Map.Entry<Table, Constraint> entry : constraints.entries()) {
            final Table table = entry.getKey();
            final Constraint constraint = entry.getValue();
            LOG.info("Restoring constraint: {} on table: {}", constraint, table);
            dbCtx.alterTable(table).add(constraint).execute();
        }
    }

    /**
     * Removes existing foreign key constraints
     * @return Returns the removed constraints per table
     */
    private void removeConstraints(final Multimap<Table, Constraint> constraints) {
        for (final Map.Entry<Table, Constraint> entry : constraints.entries()) {
            final Table table = entry.getKey();
            final Constraint constraint = entry.getValue();
            LOG.info("Removing constraint: {} on table: {}", constraint, table);
            dbCtx.alterTable(table).drop(constraint).execute();
        }
    }

    /**
     * Converts an SQL Table entry to a MiniZinc table entry, parsing and storing a reference to every field
     * This includes:
     *  - Parsing foreign keys relationship between fields from different tables
     *
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

            // After adding all the MnzFields to the table, we parse the table UniqueKey
            // and link the correspondent MnzFields as fields that compose the IRTable primary key
            final IRPrimaryKey pk = new IRPrimaryKey(irTable, table.getPrimaryKey());
            irTable.setPrimaryKey(pk);

            // add table reference to maps
            jooqTableToIRTable.put(table, irTable);
            irTables.put(irTable.getName(), irTable);
        }

        // TODO: Assumes tables do not have foreign key relationships for now and that the user
        // has to specify them manually using membership constraints


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
    private void updateDataFields() {
        for (final Map.Entry<Table<? extends Record>, IRTable> entry : jooqTableToIRTable.entrySet()) {
            final Table<? extends Record> table = entry.getKey();
            final IRTable irTable = entry.getValue();
            final long start = System.nanoTime();
            final Result<? extends Record> recentData = dbCtx.selectFrom(table).fetch();
            final long select = System.nanoTime();
            irTable.updateValues(recentData);
            final long updateValues = System.nanoTime();
            LOG.info("updateDataFields for table {} took {}ns to fetch from DB, and {}ns to reflect in IRTables",
                     table.getName(), (select - start), (System.nanoTime() - updateValues));
        }
        final long updateData = System.nanoTime();
        compiler.updateData(irContext, backend);
        LOG.info("compiler.updateData() took {}ns to complete", (System.nanoTime() - updateData));
    }
}