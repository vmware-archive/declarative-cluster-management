/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.dcm.Conf;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.ModelException;
import org.dcm.compiler.monoid.MonoidComprehension;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MinizincSolver implements ISolverBackend {
    private static final String MODEL_FILENAME = "mnz_model.ftl";
    private static final String DATA_FILENAME = "mnz_data.ftl";
    private static final Logger LOG = LoggerFactory.getLogger(MinizincSolver.class);
    private static final String MNZ_UNSATISFIABLE = "=====UNSATISFIABLE=====";
    private static final String MNZ_SOLUTION_SEP = "----------";
    private static final SolverBackend MNZ_DEFAULT_SOLVER = SolverBackend.CHUFFED;
    private static final String CONF_SOLVER_KEY = "solver";
    private static final String CONF_DEBUG_MODE = "debug_mode";
    private static final String CONF_FZN_FLAGS = "fzn_flags";
    private static final int MNZ_SOLVER_TIMEOUT_MS = 1000000;
    private final SolverBackend solverToUse;
    private final boolean debugMode;
    private final String fznFlags;
    private final File modelFile;
    private final File dataFile;
    private final File stderr;
    private final File stdout;
    private final Template modelTemplate;
    private final Template dataTemplate;
    private final Set<String> stringLiteralsInModel = new HashSet<>();
    private final AtomicInteger batch = new AtomicInteger(0);


    public MinizincSolver(final File modelFile, final File dataFile, final Conf conf) {
        this.modelFile = modelFile;
        this.dataFile = dataFile;
        final String solverName = conf.getProperty(CONF_SOLVER_KEY);
        this.debugMode = conf.getProperty(CONF_DEBUG_MODE) != null;
        LOG.info("Debug mode: {}", this.debugMode);
        LOG.info("Conf: {}", conf);
        this.solverToUse =  solverName != null ? SolverBackend.valueOf(solverName) : MNZ_DEFAULT_SOLVER;
        final String fznFlags = conf.getProperty(CONF_FZN_FLAGS);
        this.fznFlags =  fznFlags != null ? fznFlags : "";
        try {
            this.stderr = File.createTempFile("mnz", "-err");
            this.stderr.deleteOnExit();
            this.stdout = File.createTempFile("mnz", "-out");
            this.stdout.deleteOnExit();
        } catch (final IOException e) {
            throw new ModelException("Model file not found or has formatting errors", e);
        }

        // Freemarker configuration
        final Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        final ClassTemplateLoader loader = new ClassTemplateLoader(this.getClass(), "/");
        cfg.setTemplateLoader(loader);
        cfg.setDefaultEncoding("UTF-8");

        // get template
        try {
            this.modelTemplate = cfg.getTemplate(MODEL_FILENAME);
            this.dataTemplate = cfg.getTemplate(DATA_FILENAME);
        } catch (final IOException e) {
            throw new ModelException("Model file not found or has formatting errors", e);
        }
    }

    @Override
    public Map<IRTable, Result<? extends Record>> runSolver(final DSLContext dbCtx,
                                                            final Map<String, IRTable> irTables) {
        final String output = runMnzSolver(solverToUse);
        return parseMnzOutput(dbCtx, irTables, output);
    }


    private void findStringLiterals(final Map<String, MonoidComprehension> nonConstraintViews,
                                    final Map<String, MonoidComprehension> constraintViews,
                                    final Map<String, MonoidComprehension> objectiveFunctions) {
        final FindStringLiterals search = new FindStringLiterals();
        nonConstraintViews.forEach((k, v) -> search.visit(v));
        constraintViews.forEach((k, v) -> search.visit(v));
        objectiveFunctions.forEach((k, v) -> search.visit(v));
        stringLiteralsInModel.addAll(search.getStringLiterals());
        Preconditions.checkArgument(!stringLiteralsInModel.contains("'null'"));
        stringLiteralsInModel.add("'null'"); // Used instead of null
    }

    @Override
    public List<String> generateModelCode(final IRContext context,
                                          final Map<String, MonoidComprehension> nonConstraintViews,
                                          final Map<String, MonoidComprehension> constraintViews,
                                          final Map<String, MonoidComprehension> objectiveFunctions) {
        findStringLiterals(nonConstraintViews, constraintViews, objectiveFunctions);
        final Map<String, List<String>> templateVars = new HashMap<>();
        final MinizincCodeGenerator visitor = new MinizincCodeGenerator();
        final List<String> arrayDeclarations = visitor.generateArrayDeclarations(context);
        templateVars.put("arrayDeclarations", arrayDeclarations);

        final List<String> nonConstraintViewCode = nonConstraintViews.entrySet().stream().flatMap(entry -> {
            final List<MonoidComprehension> comprehensions = comprehensionRewritePipeline(entry.getValue(), false);
            final List<String> result = new ArrayList<>();
            boolean generateArrayDeclaration = true;
            for (final MonoidComprehension c: comprehensions) {
                final MinizincCodeGenerator cg = new MinizincCodeGenerator(entry.getKey());
                cg.visit(c);
                result.addAll(cg.generateNonConstraintViewCode(entry.getKey(), generateArrayDeclaration));
                generateArrayDeclaration = false;
            }
            return result.stream();
        }).collect(Collectors.toList());
        templateVars.put("nonConstraintViewCode", nonConstraintViewCode);

        final List<String> constraintViewCode = constraintViews.entrySet().stream().flatMap(entry -> {
            final List<MonoidComprehension> comprehensions = comprehensionRewritePipeline(entry.getValue(), true);
            final List<String> result = new ArrayList<>();
            for (final MonoidComprehension c: comprehensions) {
                final MinizincCodeGenerator cg = new MinizincCodeGenerator(entry.getKey());
                cg.visit(c);
                result.addAll(cg.generateConstraintViewCode(entry.getKey()));
            }
            return result.stream();
        }).collect(Collectors.toList());
        templateVars.put("constraintViewCode", constraintViewCode);

        final List<String> objectiveFunctionsCode = objectiveFunctions.entrySet().stream().flatMap(entry -> {
            final List<MonoidComprehension> comprehensions = comprehensionRewritePipeline(entry.getValue(), false);
            final List<String> result = new ArrayList<>();
            for (final MonoidComprehension c: comprehensions) {
                final MinizincCodeGenerator cg = new MinizincCodeGenerator(entry.getKey());
                cg.visit(c);
                result.addAll(cg.generateObjectiveFunctionCode(entry.getKey()));
            }
            return result.stream();
        }).collect(Collectors.toList());

        // Objective functions are summed into one
        if (objectiveFunctionsCode.size() > 0) {
            final String objectiveFunction = String.join("\n             + ", objectiveFunctionsCode);
            templateVars.put("objectiveFunctionsCode",
                             Lists.newArrayList("solve maximize " + objectiveFunction + ";"));
        } else {
            templateVars.put("objectiveFunctionsCode", Lists.newArrayList("solve satisfy;"));
        }
        writeTemplateToFile(modelTemplate, modelFile, templateVars);
        return Lists.newArrayList(Iterables.concat(arrayDeclarations, nonConstraintViewCode,
                                                   constraintViewCode, objectiveFunctionsCode));
    }

    @Override
    public List<String> generateDataCode(final IRContext context) {
        final List<String> ret = new ArrayList<>();
        final Map<String, List<String>> templateVars = new HashMap<>();
        final Set<String> stringLiterals = new HashSet<>(stringLiteralsInModel);
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }

            // adds declaration for the number of rows for that table
            ret.add(String.format("%% %s Table", table.getName()));
            ret.add(String.format("%s = %s;", MinizincString.tableNumRowsName(table), table.getNumRows()));

            // Fields
            for (final Map.Entry<String, IRColumn> fieldEntrySet: table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();

                if (field.isControllable()) {
                    continue;
                }
                ret.add(String.format("%% %s", fieldName));
                ret.add(String.format("%s = [ %s ];",
                        MinizincString.qualifiedName(field),
                        // if the field is a string, wraps the values around the INDEX function
                        field.getValues().stream()
                                .map(v -> {
                                    if (field.isString()) {
                                        final String replacedString =
                                                "'" + v.replaceAll("\"", "") + "'";
                                        stringLiterals.add(replacedString);
                                        return replacedString;
                                    } else {
                                        return v == null || v.equalsIgnoreCase("null") ? "'null'" : v;
                                    }
                                })
                                .collect(Collectors.joining(" , "))
                        )
                );
            }
        }
        templateVars.put("string_literals", new ArrayList<>(stringLiterals));
        templateVars.put("input_parameters", ret);
        writeTemplateToFile(dataTemplate, dataFile, templateVars);
        return ret;
    }

    @Override
    public boolean needsGroupTables() {
        return true;
    }

    private List<MonoidComprehension> comprehensionRewritePipeline(final MonoidComprehension comprehension,
                                                                   final boolean isConstraint) {
        // (1) Split into multiple comprehensions, one per head
        // (2) Rewrite count functions to use sums instead
        // (3) Rewrite to use fixed arity constraints
        // (4) Rewrite IsNull/IsNotNull expressions to use 'null' strings
        final List<MonoidComprehension> comprehensions = SplitIntoSingleHeadComprehensions.apply(comprehension); // (1)
        return (isConstraint
                 ? Collections.singletonList(comprehensions.get(0)) // We only need one head item for constraints
                 : comprehensions)
                .stream()
                .map(RewriteCountFunction::apply) // (2)
                .map(RewriteArity::apply) // (3)
                .map(RewriteNullPredicates::apply) // (4)
                .collect(Collectors.toList());
    }


    private enum SolverBackend {
        // different solvers use different timeout flags
        GECODE("Gecode"),
        CHUFFED("Chuffed"),
        ORTOOLS("or-tools");

        private final String solver;

        SolverBackend(final String solver) {
            this.solver = solver;
        }

        public String getSolver() {
            return solver;
        }

        public ProcessBuilder getCmd(final int timeout, final String modelFile,
                                     final String dataFile, final String fznFlags) {
            switch (SolverBackend.this) {
                case GECODE:
                case CHUFFED:
                    return new ProcessBuilder("minizinc", "--solver", solver,
                        "--time-limit", Integer.toString(timeout), "--num-solutions", "1",
                        modelFile, dataFile);
                case ORTOOLS:
                    final String fznString =
                            String.format("-time-limit %s -num_solutions 1 %s", timeout, fznFlags).trim();
                    return new ProcessBuilder("minizinc", "-Gminizinc_sat", "--solver", solver,
                        "--fzn-flags", fznString,
                        modelFile, dataFile);
                default:
                    throw new IllegalArgumentException(solver);
            }
        }
    }


    /**
     * Parses MiniZinc output splitting it into one CSV for each table.
     *
     * Sample output:
     *      !!HOSTS
     *      HOST_ID,CONTROLLABLE__IN_SEGMENT
     *      "h1",true
     *      "h2",true
     *
     * @return Map with table to CSVParser
     */
    private Map<IRTable, Result<? extends Record>> parseMnzOutput(final DSLContext dbCtx,
                                                                  final Map<String, IRTable> irTables,
                                                                  final String output) {
        final Map<IRTable, Result<? extends Record>> csvPerTable = new HashMap<>();
        // we split tables by a specific tag
        for (final String tableLine : Splitter.on(MinizincString.MNZ_OUTPUT_TABLENAME_TAG)
                .omitEmptyStrings().split(output)) {
            try {
                // and now we split output in 2 parts
                final List<String> tableParts = Splitter.on("\n").limit(2).splitToList(tableLine);
                // - 1st line: table name
                final String tableName = tableParts.get(0);
                final IRTable irTable = irTables.get(tableName);
                if (irTable == null) {
                    LOG.error("Null irtable with batch-ID {}, tablename {} and " +
                              "line {}", batch.get(), tableLine, tableName);
                    throw new ModelException("Null irtable");
                }
                // - 2nd to nth line: CSV with header
                // TODO: workaround for single-quoted strings
                final String csvWithHeader = tableParts.get(1).replace("'", "\"");
                final Result<? extends Record> records = dbCtx
                        .fetchFromCSV(csvWithHeader, true, MinizincString.MNZ_OUTPUT_CSV_DELIMITER)
                        .into(irTable.getTable());
                csvPerTable.put(irTable, records);
            } catch (final IndexOutOfBoundsException e) {
                throw new ModelException("Mal-formed output!", e);
            }
        }
        return csvPerTable;
    }

    /**
     * Runs the MiniZinc solver and returns a pre-verified output.
     * We check for UNSATISFIABLE, remove standard MiniZinc and empty lines
     *
     * @return Returns a pre-parsed output string from the MiniZinc solver
     */
    private String runMnzSolver(final SolverBackend solver) {
        final Process mnz;
        try {
            try {
                final ProcessBuilder pb = solver.getCmd(MNZ_SOLVER_TIMEOUT_MS,
                        modelFile.getAbsolutePath(),
                        dataFile.getAbsolutePath(),
                        fznFlags);
                LOG.info("Running command {}", pb.command());

                mnz = pb.redirectError(stderr).redirectOutput(stdout).start();
            } catch (final IOException e) {
                throw new ModelException("Could not execute MiniZinc", e);
            }
            // wait for minizinc to solve the model
            try {
                mnz.waitFor();
            } catch (final InterruptedException e) {
                throw new ModelException("MiniZinc was interrupted", e);
            }

            LOG.info("Solver command completed. Parsing output.");
            // Throw exception if minizinc throws an error exit code
            if (mnz.exitValue() != 0) {
                try (BufferedReader stdError =
                             new BufferedReader(new InputStreamReader(new FileInputStream(stderr), UTF_8))) {
                    final String errorOutput = stdError.lines().collect(Collectors.joining());
                    if (!errorOutput.contains("WARNING: the --time-out flag has recently been changed")) {
                        throw new ModelException(
                                String.format("MiniZinc exited with error code %d:%n%s", mnz.exitValue(), errorOutput));
                    }
                } catch (final IOException ioe) {
                    throw new ModelException("Could not execute MiniZinc", ioe);
                }
            }

            LOG.info("Before string joiner");

            // parse input
            final StringJoiner output = new StringJoiner("\n");
            // adds empty line so we can then split by "\nOUTPUT_TABLENAME_TAG"
            output.add("");
            try (BufferedReader stdInput =
                         new BufferedReader(new InputStreamReader(new FileInputStream(stdout), UTF_8))) {
                String line;
                while ((line = stdInput.readLine()) != null) {
                    // if we find the unsatisfiable line in the output we throw an exception
                    if (line.equals(MNZ_UNSATISFIABLE)) {
                        throw new ModelException("Model UNSATISFIABLE. Please verify your " +
                                                                 "model and data.");
                    }
                    // break at first solution. Since we are not specifying '--all-solutions' the solver
                    // will only output the last solution
                    if (line.equals(MNZ_SOLUTION_SEP)) {
                        break;
                    }
                    // ignore empty lines (includes whitespace-only lines)
                    if (line.trim().length() <= 0) {
                        continue;
                    }
                    output.add(line);
                }
                // LOG.info("MNZ OUTPUT = " + output.toString());
                return output.toString();
            } catch (final IOException ioe) {
                throw new ModelException("Could not execute MiniZinc", ioe);
            }
        } finally {
            if (debugMode) {
                final Path path = dataFile.toPath();
                final Path dest = FileSystems.getDefault().getPath("/tmp",
                        String.format("debug_data_%s.dzn", batch.incrementAndGet()));
                try {
                    Files.copy(path, dest, StandardCopyOption.REPLACE_EXISTING);
                } catch (final IOException e) {
                    LOG.error("Debug-mode: could not copy file", e);
                }
            }
        }
    }

    /**
     * Populates a file based on a Apache FreeMarker template
     */
    private void writeTemplateToFile(final Template template, final File templateFile,
                                     final Map<String, List<String>> templateVars) {
        try {
            final Writer writer = Files.newBufferedWriter(templateFile.toPath(), UTF_8);
            template.process(templateVars, writer);
        } catch (final TemplateException | IOException e) {
            throw new ModelException("Error processing template", e);
        }
    }
}
