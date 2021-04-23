/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.util.Domain;
import com.skaggsm.ortools.OrToolsHelper;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRContext;
import com.vmware.dcm.IRPrimaryKey;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.SolverException;
import com.vmware.dcm.backend.GetVarQualifiers;
import com.vmware.dcm.backend.IGeneratedBackend;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.backend.RewriteArity;
import com.vmware.dcm.backend.RewriteContains;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicateWithAggregate;
import com.vmware.dcm.compiler.ir.CheckQualifier;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.GroupByQualifier;
import com.vmware.dcm.compiler.ir.IRVisitor;
import com.vmware.dcm.compiler.ir.IsNotNullPredicate;
import com.vmware.dcm.compiler.ir.IsNullPredicate;
import com.vmware.dcm.compiler.ir.JoinPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.Qualifier;
import com.vmware.dcm.compiler.ir.TableRowGenerator;
import com.vmware.dcm.compiler.ir.UnaryOperator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.processing.Generated;
import javax.lang.model.element.Modifier;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * This class generates Java code that invokes the or-tools CP-SAT solver. The generated
 * class is compiled, and loaded via ReflectASM to return an IGeneratedBackend instance,
 * which can be invoked to run the solver.
 */
public class OrToolsSolver implements ISolverBackend {
    private static final int NUM_THREADS_DEFAULT = 4;
    private static final int MAX_TIME_IN_SECONDS = 1;
    private static final String GENERATED_BACKEND_CLASS_FILE_PATH = "/tmp";
    private static final Logger LOG = LoggerFactory.getLogger(OrToolsSolver.class);
    private static final String GENERATED_BACKEND_NAME = "GeneratedBackend";
    private static final MethodSpec INT_VAR_NO_BOUNDS = MethodSpec.methodBuilder("INT_VAR_NO_BOUNDS")
                                    .addModifiers(Modifier.PRIVATE)
                                    .addParameter(CpModel.class, "model", Modifier.FINAL)
                                    .addParameter(String.class, "name",  Modifier.FINAL)
                                    .returns(IntVar.class)
                                    .addStatement("return model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, name)")
                                    .build();
    private final AtomicInteger intermediateViewCounter = new AtomicInteger(0);
    private final TupleMetadata tupleMetadata = new TupleMetadata();
    private final TupleGen tupleGen = new TupleGen();
    private final OutputIR outputIR = new OutputIR();
    private final int configNumThreads;
    private final int configMaxTimeInSeconds;
    private final boolean configTryScalarProductEncoding;
    private final boolean configUseIndicesForEqualityBasedJoins;
    private final boolean configPrintDiagnostics;

    static {
        OrToolsHelper.loadLibrary();
    }

    @Nullable private IGeneratedBackend generatedBackend;
    @Nullable private IRContext context = null;

    private OrToolsSolver(final int configNumThreads, final int configMaxTimeInSeconds,
                          final boolean configTryScalarProductEncoding,
                          final boolean configUseIndicesForEqualityBasedJoins,
                          final boolean configPrintDiagnostics) {
        this.configNumThreads = configNumThreads;
        this.configMaxTimeInSeconds = configMaxTimeInSeconds;
        this.configTryScalarProductEncoding = configTryScalarProductEncoding;
        this.configUseIndicesForEqualityBasedJoins = configUseIndicesForEqualityBasedJoins;
        this.configPrintDiagnostics = configPrintDiagnostics;
    }

    public static class Builder {
        private int numThreads = NUM_THREADS_DEFAULT;
        private int maxTimeInSeconds = MAX_TIME_IN_SECONDS;
        private boolean tryScalarProductEncoding = true;
        private boolean useIndicesForEqualityBasedJoins = true;
        private boolean printDiagnostics = false;


        /**
         * Number of solver threads. Corresponds to CP-SAT's setNumSearchWorkers parameter.
         * @param numThreads number of solver threads to use. Defaults to {@value NUM_THREADS_DEFAULT}.
         * @return the current Builder object with `numThreads` set
         */
        public Builder setNumThreads(final int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        /**
         * Solver timeout. If this parameter is set too low for the problem size involved, expect a ModelException
         * for not finding a solution. Corresponds to CP-SAT's setNumSearchWorkers parameter.
         * @param maxTimeInSeconds timeout value in seconds. Defaults to {@value MAX_TIME_IN_SECONDS}.
         * @return the current Builder object with `maxTimeInSeconds` set
         */
        public Builder setMaxTimeInSeconds(final int maxTimeInSeconds) {
            this.maxTimeInSeconds = maxTimeInSeconds;
            return this;
        }

        /**
         * Configures whether we attempt to pattern match and apply an optimization that uses scalar products
         * in certain kinds of aggregates.
         * @param tryScalarProductEncoding true to apply scalar product optimization. Defaults to true.
         * @return the current Builder object with `tryScalarProductEncoding` set
         */
        public Builder setTryScalarProductEncoding(final boolean tryScalarProductEncoding) {
            this.tryScalarProductEncoding = tryScalarProductEncoding;
            return this;
        }

        /**
         * Configures whether we construct and leverage indexes for equality based joins, instead of nested for
         * loops in the generated code.
         *
         * @param useIndicesForEqualityBasedJoins generated code uses indexes if possible. Defaults to true.
         * @return the current Builder object with `useIndicesForEqualityBasedJoins` set
         */
        public Builder setUseIndicesForEqualityBasedJoins(final boolean useIndicesForEqualityBasedJoins) {
            this.useIndicesForEqualityBasedJoins = useIndicesForEqualityBasedJoins;
            return this;
        }


        /**
         * Configures whether or not to print diagnostics for each invocation of solve()
         *
         * @param printDiagnostics generated code prints timing and solver diagnostics on each invocation.
         *                         Defaults to true.
         * @return the current Builder object with `printDiagnostics` set
         */
        public Builder setPrintDiagnostics(final boolean printDiagnostics) {
            this.printDiagnostics = printDiagnostics;
            return this;
        }

        public OrToolsSolver build() {
            return new OrToolsSolver(numThreads, maxTimeInSeconds, tryScalarProductEncoding,
                                     useIndicesForEqualityBasedJoins, printDiagnostics);
        }
    }

    @Override
    public Map<IRTable, Result<? extends Record>> runSolver(final DSLContext dbCtx,
                                                            final Map<String, IRTable> irTables) {
        Preconditions.checkNotNull(generatedBackend);
        Preconditions.checkNotNull(context);
        return generatedBackend.solve(context);
    }

    /**
     * This method is where the code generation happens.
     *
     * It generates an instance of IGeneratedBackend with a solve() method. The solve method
     * creates all the IntVar instances required for variable columns, and translates comprehensions
     * into nested for loops. It re-uses JOOQ tables wherever possible for constants.
     */
    @Override
    public List<String> generateModelCode(final IRContext context,
                                          final Map<String, ListComprehension> nonConstraintViews,
                                          final Map<String, ListComprehension> constraintViews,
                                          final Map<String, ListComprehension> objectiveFunctions) {
        if (generatedBackend != null) {
            return Collections.emptyList();
        }

        final MethodSpec.Builder output = MethodSpec.methodBuilder("solve");

        addInitializer(output);
        try {
            addArrayDeclarations(output, context);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        final TranslationContext translationContext = new TranslationContext(false);
        nonConstraintViews
                .forEach((name, comprehension) -> {
                    final ListComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    final OutputIR.Block outerBlock = outputIR.newBlock("outer");
                    translationContext.enterScope(outerBlock);
                    final OutputIR.Block block = viewBlock(name, rewrittenComprehension, false, translationContext);
                    output.addCode(translationContext.leaveScope().toString());
                    output.addCode(block.toString());
                });
        constraintViews
                .forEach((name, comprehension) -> {
                    final List<FunctionCall> capacityConstraints = DetectCapacityConstraints.apply(comprehension);
                    if (capacityConstraints.isEmpty()) {
                        final ListComprehension rewrittenComprehension = rewritePipeline(comprehension);
                        final OutputIR.Block outerBlock = outputIR.newBlock("outer");
                        translationContext.enterScope(outerBlock);
                        final OutputIR.Block block = viewBlock(name, rewrittenComprehension, true, translationContext);
                        output.addCode(translationContext.leaveScope().toString());
                        output.addCode(block.toString());
                    } else {
                        final OutputIR.Block outerBlock = outputIR.newBlock("outer");
                        translationContext.enterScope(outerBlock);
                        final OutputIR.Block block = createCapacityConstraint(name, comprehension, translationContext,
                                                                              capacityConstraints);
                        translationContext.currentScope().addBody(block);
                        translationContext.leaveScope();
                        output.addCode(block.toString());
                    }
                });
        objectiveFunctions
                .forEach((name, comprehension) -> {
                    final ListComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    final OutputIR.Block outerBlock = outputIR.newBlock("outer");
                    translationContext.enterScope(outerBlock);
                    final String exprStr = exprToStr(rewrittenComprehension, translationContext);
                    output.addCode(outerBlock.toString());
                    output.addStatement("o.maximize($L)", exprStr);
                });
        addSolvePhase(output, context);
        final MethodSpec solveMethod = output.build();

        final TypeSpec.Builder backendClassBuilder = TypeSpec.classBuilder(GENERATED_BACKEND_NAME)
                .addAnnotation(AnnotationSpec.builder(Generated.class)
                                 .addMember("value", "$S", "com.vmware.dcm.backend.ortools.OrToolsSolver")
                                 .build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(IGeneratedBackend.class)
                .addMethod(solveMethod)
                .addMethod(INT_VAR_NO_BOUNDS);
        tupleGen.getAllTupleTypes().forEach(backendClassBuilder::addType); // Add tuple types

        final TypeSpec spec = backendClassBuilder.build();
        return compile(spec);
    }

    /**
     * Creates and populates a block, representing a view. This corresponds to one or more sets of nested
     * for loops and result sets.
     * @param viewName name of the view to be created
     * @param comprehension comprehension corresponding to `viewName`, to be translated into a block of code
     * @param isConstraint whether the comprehension represents a constraint
     * @param context the translation context for the comprehension
     * @return the block created for the comprehension
     */
    private OutputIR.Block viewBlock(final String viewName, final ListComprehension comprehension,
                                     final boolean isConstraint, final TranslationContext context) {
        if (comprehension instanceof GroupByComprehension) {
            final GroupByComprehension groupByComprehension = (GroupByComprehension) comprehension;
            final ListComprehension inner = groupByComprehension.getComprehension();
            final GroupByQualifier groupByQualifier = groupByComprehension.getGroupByQualifier();
            final OutputIR.Block block = outputIR.newBlock(viewName);

            // Rewrite inner comprehension using the following rule:
            // [ [blah - sum(X) | <....>] | G]
            //                     rows
            //

            // We create an intermediate view that extracts groups and returns a List<Tuple> per group
            final String intermediateView = getTempViewName();
            final OutputIR.Block intermediateViewBlock =
                    innerComprehensionBlock(intermediateView, inner, groupByQualifier, isConstraint, context);
            block.addBody(intermediateViewBlock);

            // We now construct the actual result set that hosts the aggregated tuples by group. This is done
            // in two steps...

            // (1) Create the result set
            block.addBody(CodeBlock.builder()
                                 .add("\n")
                                 .addStatement(printTime("Group-by intermediate view"))
                                 .add("/* $L view $L */\n", isConstraint ? "Constraint" : "Non-constraint",
                                                    tableNameStr(viewName))
                                 .build()
            );

            if (!isConstraint) {
                final int selectExprSize = inner.getHead().getSelectExprs().size();
                final TypeSpec typeSpec = tupleGen.getTupleType(selectExprSize);
                final JavaTypeList viewTupleGenericParameters =
                        tupleMetadata.computeViewTupleType(tableNameStr(viewName), inner.getHead().getSelectExprs());
                block.addBody(
                    statement("final $T<$N<$L>> $L = new $T<>($L.size())", List.class, typeSpec,
                                                     viewTupleGenericParameters,
                                                     tableNameStr(viewName),
                                                     ArrayList.class, intermediateView)
                );
            }

            // (2) Loop over the result set collected from the inner comprehension
            final OutputIR.ForBlock forBlock = outputIR.newForBlock(viewName,
                    CodeBlock.of("for (final var entry: $L.entrySet())", intermediateView)
            );
            forBlock.addHeader(CodeBlock.builder().addStatement("final var group = entry.getKey()")
                                                  .addStatement("final var data = entry.getValue()").build());

            final OutputIR.ForBlock dataForBlock = outputIR.newForBlock(viewName,
                        CodeBlock.of("for (final var dataTuple: data)"), "data.size()");
            forBlock.addBody(dataForBlock);

            final GroupContext groupContext = new GroupContext(groupByQualifier, intermediateView, viewName);
            context.enterScope(forBlock);

            // (3) Filter if necessary
            final QualifiersByVarType qualifiersByVarType = extractQualifiersByVarType(inner, false);
            final Optional<OutputIR.IfBlock> nonVarAggregateFiltersBlock = nonVarAggregateFiltersBlock(viewName,
                    qualifiersByVarType.nonVar, groupContext, context);
            nonVarAggregateFiltersBlock.ifPresent(forBlock::addBody);

            // If this is not a constraint, we simply add a to a result set
            if (!isConstraint) {
                final TypeSpec typeSpec = tupleGen.getTupleType(inner.getHead().getSelectExprs().size());
                final String tupleResult = inner.getHead().getSelectExprs().stream()
                                                 .map(e -> exprToStr(e, true, groupContext, context))
                                                 .collect(Collectors.joining(", "));
                forBlock.addBody(statement("final var $2L = new $1N<>($3L)",
                                            typeSpec, context.getTupleVarName(), tupleResult));

                // Record field name indices for view
                tupleMetadata.recordFieldIndices(viewName, inner.getHead().getSelectExprs());
                forBlock.addBody(statement("$L.add($L)", tableNameStr(viewName), context.getTupleVarName()));
            }
            else  {
                // If this is a constraint, we translate having clauses into a constraint statement
                final List<CodeBlock> constraintBlocks = aggregateConstraintBlock(qualifiersByVarType,
                        new GroupContext(groupByQualifier, intermediateView, viewName), context);
                constraintBlocks.forEach(forBlock::addBody);
            }
            context.leaveScope(); // forBlock
            block.addBody(forBlock);
            block.addBody(printTime("Group-by final view"));
            return block;
        }
        return innerComprehensionBlock(viewName, comprehension, null, isConstraint, context);
    }

    /**
     * Converts a comprehension into a set of nested for loops that return a "result set". The result set
     * is represented as a list of tuples.
     */
    private OutputIR.Block innerComprehensionBlock(final String viewName,
                                                   final ListComprehension comprehension,
                                                   @Nullable final GroupByQualifier groupByQualifier,
                                                   final boolean isConstraint, final TranslationContext context) {
        final OutputIR.Block viewBlock = outputIR.newBlock(viewName);
        // Add a comment with the view name
        viewBlock.addHeader(CodeBlock.builder().add("\n")
                     .add("/* $L view $L */\n", isConstraint ? "Constraint" : "Non-constraint", viewName)
                     .build()
        );

        // Extract the set of columns being selected in this view
        final List<ColumnIdentifier> headItemsList = getColumnsAccessed(comprehension, isConstraint);
        tupleMetadata.recordFieldIndices(viewName, headItemsList);

        context.enterScope(viewBlock);

        // For non-constraints, create a Map<> or a List<> to collect the result-set (depending on
        // whether the query is a group by or not)
        final OutputIR.Block resultSetDeclBlock = mapOrListForResultSetBlock(viewName, headItemsList,
                                                                             groupByQualifier, isConstraint);

        // Separate out qualifiers into variable and non-variable types.
        final QualifiersByVarType qualifiersByVarType = extractQualifiersByVarType(comprehension, true);

        // Start control flows to iterate over tables/views
        final OutputIR.ForBlock iterationBlock = tableIterationBlock(viewName, qualifiersByVarType.nonVar);
        context.enterScope(iterationBlock);

        // Filter out nested for loops using an if(predicate) statement
        final Optional<OutputIR.IfBlock> nonVarFiltersBlock = nonVarFiltersBlock(viewName, qualifiersByVarType.nonVar,
                                                                                 context);
        viewBlock.addBody(resultSetDeclBlock);
        viewBlock.addBody(iterationBlock);
        nonVarFiltersBlock.ifPresent(iterationBlock::addBody);
        if (!isConstraint // for simple constraints, we post constraints in this inner loop itself
           || (groupByQualifier != null) // for aggregate constraints, we populate a
                                         // result set and post constraints elsewhere
        ) {
            // If filter predicate is true, then retrieve expressions to collect into result set. Note, this does not
            // evaluate things like functions (sum etc.). These are not aggregated as part of the inner expressions.

            final OutputIR.Block resultSetAddBlock = resultSetAddBlock(viewName, headItemsList,
                                                                       groupByQualifier, context);
            iterationBlock.addBody(resultSetAddBlock);
        } else {
            final List<CodeBlock> addRowConstraintBlock = rowConstraintBlock(qualifiersByVarType, context);
            addRowConstraintBlock.forEach(iterationBlock::addBody);
        }
        context.leaveScope(); // iteration block
        context.leaveScope(); // view block
        return viewBlock;
    }

    /**
     * Gets the set of columns accessed within the current scope of a comprehension (for example, without entering
     * sub-queries)
     *
     * @param comprehension comprehension to search for columns
     * @param isConstraint whether the comprehension represents a constraint or not
     * @return a list of ColumnIdentifiers corresponding to columns within the comprehension
     */
    private List<ColumnIdentifier> getColumnsAccessed(final ListComprehension comprehension,
                                                      final boolean isConstraint) {
        if (isConstraint) {
            final List<ColumnIdentifier> columnsAccessed = getColumnsAccessed(comprehension.getQualifiers());
            if (columnsAccessed.isEmpty()) {
                // There are constraints that are trivially true or false, wherein the predicate does not depend on
                // on any columns from the relations in the query. See the ModelTest.innerSubqueryCountTest
                // as an example. In such cases, we simply revert to pulling out all the head items for the outer query.
                return getColumnsAccessed(comprehension.getHead().getSelectExprs());
            }
            return columnsAccessed;
        } else {
            final List<ColumnIdentifier> columnsFromQualifiers = getColumnsAccessed(comprehension.getQualifiers());
            final List<ColumnIdentifier> columnsFromHead = getColumnsAccessed(comprehension.getHead().getSelectExprs());
            return Lists.newArrayList(Iterables.concat(columnsFromHead, columnsFromQualifiers));
        }
    }

    private List<ColumnIdentifier> getColumnsAccessed(final List<? extends Expr> exprs) {
        return exprs.stream()
                .map(this::getColumnsAccessed)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    private LinkedHashSet<ColumnIdentifier> getColumnsAccessed(final Expr expr) {
        final GetColumnIdentifiers visitor = new GetColumnIdentifiers(false);
        visitor.visit(expr);
        return visitor.getColumnIdentifiers();
    }

    /**
     * If required, returns a block of code representing maps or lists created to host the result-sets returned
     * by a view.
     */
    private OutputIR.Block mapOrListForResultSetBlock(final String viewName, final List<ColumnIdentifier> headItemsList,
                                                      @Nullable final GroupByQualifier groupByQualifier,
                                                      final boolean isConstraint) {
        final OutputIR.Block block = outputIR.newBlock(viewName + "CreateResultSet");

        // Compute a string that represents the Java types corresponding to the headItemsStr
        final JavaTypeList headItemsListTupleGenericParameters =
                tupleMetadata.computeViewTupleType(viewName, headItemsList);

        final TypeSpec tupleSpec = tupleGen.getTupleType(headItemsList.size());
        final String resultSetNameStr = nonConstraintViewName(viewName);

        if (groupByQualifier != null) {
            // Create group by tuple
            final int numberOfGroupColumns = groupByQualifier.getGroupByExprs().size();
            final JavaTypeList groupByTupleGenericParameters =
                    tupleMetadata.computeGroupByTupleType(viewName, groupByQualifier.getGroupByExprs());
            final TypeSpec groupTupleSpec = tupleGen.getTupleType(numberOfGroupColumns);
            block.addHeader(statement("final Map<$N<$L>, $T<$N<$L>>> $L = new $T<>()",
                                        groupTupleSpec,
                                        groupByTupleGenericParameters,
                                        List.class, tupleSpec,
                                        headItemsListTupleGenericParameters,
                                        resultSetNameStr, HashMap.class));
        } else {
            if (!isConstraint) {
                block.addHeader(statement("final $T<$N<$L>> $L = new $T<>()",
                                          List.class, tupleSpec, headItemsListTupleGenericParameters,
                                          resultSetNameStr, ArrayList.class));
            }
        }
        return block;
    }

    /**
     * Translates a TableRowGenerator into a for loop statement
     */
    private CodeBlock forLoopFromTableRowGeneratorBlock(final TableRowGenerator tableRowGenerator) {
        final String tableName = tableRowGenerator.getTable().getName();
        final String tableNumRowsStr = tableNumRowsStr(tableName);
        final String iterStr = iterStr(tableRowGenerator.getTable().getAliasedName());
        return CodeBlock.of("for (int $1L = 0; $1L < $2L; $1L++)", iterStr, tableNumRowsStr);
    }

    /**
     * Returns a block of code representing a selection or a join. The block brings into scope
     * iteration indices pointing to the relevant lists of tuples. These iteration indices
     * may be obtained via nested for loops or using indexes if available.
     */
    private OutputIR.ForBlock tableIterationBlock(final String viewName,
                                               final QualifiersByType nonVarQualifiers) {
        final List<CodeBlock> loopStatements = forLoopsOrIndicesFromTableRowGenerators(
                                                        nonVarQualifiers.tableRowGenerators,
                                                        nonVarQualifiers.joinPredicates);
        return outputIR.newForBlock(viewName, loopStatements);
    }


    private List<CodeBlock> forLoopsOrIndicesFromTableRowGenerators(final List<TableRowGenerator> tableRowGenerators,
                                                                         final List<JoinPredicate> joinPredicates) {
        final TableRowGenerator forLoopTable = tableRowGenerators.get(0);
        final List<CodeBlock> loopStatements = new ArrayList<>();
        loopStatements.add(forLoopFromTableRowGeneratorBlock(forLoopTable));

        // For now, we always use a scan for the first Table being iterated over.
        // XXX: It would be better to look at all join predicates and determine which tables should be scanned
        tableRowGenerators.subList(1, tableRowGenerators.size()).stream()
            .map(tr -> {
                if (configUseIndicesForEqualityBasedJoins && tr.getUniquePrimaryKeyColumn().isPresent()) {
                    // We might be able to use an index here, look for equality based accesses
                    // across the join predicates
                    for (final BinaryOperatorPredicate binaryOp: joinPredicates) {
                        if (binaryOp.getOperator().equals(BinaryOperatorPredicate.Operator.EQUAL)) {
                            final ColumnIdentifier left = (ColumnIdentifier) binaryOp.getLeft();
                            final ColumnIdentifier right = (ColumnIdentifier) binaryOp.getRight();

                            if (left.getTableName().equals(forLoopTable.getTable().getName())
                                    && right.getTableName().equals(tr.getTable().getName())) {
                                return indexedAccess(tr, forLoopTable, left);
                            } else if (right.getTableName().equals(forLoopTable.getTable().getName())
                                    && left.getTableName().equals(tr.getTable().getName())) {
                                return indexedAccess(tr, forLoopTable, right);
                            }
                        }
                    }
                }
                // By default, we'll stick to producing nested for loops
                LOG.warn("{} are being iterated using nested for loops", tableRowGenerators);
                return forLoopFromTableRowGeneratorBlock(tr);
            })
            .forEach(loopStatements::add);
        return loopStatements;
    }

    private CodeBlock indexedAccess(final TableRowGenerator indexedTable, final TableRowGenerator scanTable,
                                    final ColumnIdentifier scanColumn) {
        final String idxIterStr = iterStr(indexedTable.getTable().getAliasedName());
        final String idxTableNameStr = tableNameStr(indexedTable.getTable().getName());
        final String fieldAccessFromScan = fieldNameStrWithIter(scanTable.getTable().getName(),
                                                                scanColumn.getField().getName(),
                                                                iterStr(scanTable.getTable().getAliasedName()));
        return CodeBlock.builder()
                .addStatement("final Integer $1L = $2LIndex.get($3L)", idxIterStr, idxTableNameStr, fieldAccessFromScan)
                .beginControlFlow("if ($L == null)", idxIterStr)
                .addStatement("continue")
                .endControlFlow()
                .build();
    }

    /**
     * Returns a block of code representing an if statement that evaluates constant predicates to determine
     * whether a result-set or constraint applies to a row within a view
     */
    private Optional<OutputIR.IfBlock> nonVarFiltersBlock(final String viewName,
                                                          final QualifiersByType nonVarQualifiers,
                                                          final TranslationContext context) {
        final String joinPredicateStr = nonVarQualifiers.joinPredicates.stream()
                .map(expr -> exprToStr(expr, false, null, context))
                .collect(Collectors.joining(" \n    && "));
        final String wherePredicateStr = nonVarQualifiers.wherePredicates.stream()
                                                .map(expr -> exprToStr(expr, false, null, context))
                                                .collect(Collectors.joining(" \n    && "));
        final String predicateStr = Stream.of(joinPredicateStr, wherePredicateStr)
                .filter(s -> !s.equals(""))
                .collect(Collectors.joining(" \n    && "));

        if (!predicateStr.isEmpty()) {
            // Add filter predicate if available
            final String nonVarFilter = CodeBlock.of("if (!($L))", predicateStr).toString();
            return Optional.of(outputIR.newIfBlock(viewName + "nonVarFilter", nonVarFilter));
        }
        return Optional.empty();
    }

    /**
     * Returns a block of code representing an if statement that evaluates constant aggregate predicates to determine
     * whether a result-set or constraint applies to a row within a view
     */
    private Optional<OutputIR.IfBlock> nonVarAggregateFiltersBlock(final String viewName,
                                                                   final QualifiersByType nonVarQualifiers,
                                                                   final GroupContext groupContext,
                                                                   final TranslationContext translationContext) {
        final String predicateStr = nonVarQualifiers.aggregatePredicates.stream()
                .map(expr -> exprToStr(expr, false, groupContext, translationContext))
                .collect(Collectors.joining(" \n    && "));

        if (!predicateStr.isEmpty()) {
            // Add filter predicate if available
            final String nonVarFilter = CodeBlock.of("if ($L)", predicateStr).toString();
            return Optional.of(outputIR.newIfBlock(viewName + "nonVarFilter", nonVarFilter));
        }
        return Optional.empty();
    }

    /**
     * Returns a block of code representing an addition of a tuple to a result-set.
     *
     * TODO: this overlaps with newly added logic to extract vectors from tuples. They currently perform redundant
     *  work that results in additional lists and passes over the data being created.
     */
    private OutputIR.Block resultSetAddBlock(final String viewName, final List<ColumnIdentifier> headItems,
                                             @Nullable final GroupByQualifier groupByQualifier,
                                             final TranslationContext context) {
        final OutputIR.Block block = outputIR.newBlock(viewName + "AddToResultSet");
        final String headItemsStr = headItems.stream()
                .map(expr -> exprToStr(expr, context) + " /* " + expr.getField().getName() + " */")
                .collect(Collectors.joining(",\n    "));
        // Create a tuple for the result set
        block.addBody(statement("final var $2L = new Tuple$1L<>(\n    $3L\n    )",
                                headItems.size(), context.getTupleVarName(), headItemsStr));
        final String resultSetNameStr = nonConstraintViewName(viewName);

        // Update result set
        if (groupByQualifier != null) {
            final int numberOfGroupByColumns = groupByQualifier.getGroupByExprs().size();
            // Comma separated list of field accesses to construct a group string
            final String groupString = groupByQualifier.getGroupByExprs().stream()
                    .map(e -> exprToStr(e, context))
                    .collect(Collectors.joining(",     \n"));

            // Organize the collected tuples from the nested for loops by groupByTuple
            block.addBody(statement("final var groupByTuple = new Tuple$1L<>(\n    $2L\n    )",
                           numberOfGroupByColumns, groupString));
            block.addBody(statement("$L.computeIfAbsent(groupByTuple, (k) -> new $T<>()).add($L)",
                                     resultSetNameStr, ArrayList.class, context.getTupleVarName()));
        } else {
            block.addBody(statement("$L.add($L)", resultSetNameStr, context.getTupleVarName()));
        }
        return block;
    }

    /**
     * Returns a list of code blocks representing constraints posted against rows within a view
     */
    private List<CodeBlock> rowConstraintBlock(final QualifiersByVarType qualifiersByVarType,
                                               final TranslationContext context) {
        final QualifiersByType varQualifiers = qualifiersByVarType.var;
        final QualifiersByType nonVarQualifiers = qualifiersByVarType.nonVar;
        final String joinPredicateStr;
        if (varQualifiers.joinPredicates.size() > 0) {
            final BinaryOperatorPredicate combinedJoinPredicate = varQualifiers.joinPredicates
                    .stream()
                    .map(e -> (BinaryOperatorPredicate) e)
                    .reduce(varQualifiers.joinPredicates.get(0),
                       (left, right) -> new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.AND, left, right));
            joinPredicateStr = exprToStr(combinedJoinPredicate, true, null, context);
        } else {
            joinPredicateStr = "";
        }
        final List<CodeBlock> results = new ArrayList<>();
        varQualifiers.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), joinPredicateStr, null, context)));
        nonVarQualifiers.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), joinPredicateStr, null, context)));
        return results;
    }

    /**
     * Returns a list of code blocks representing aggregate constraints posted against rows within a view
     */
    private List<CodeBlock> aggregateConstraintBlock(final QualifiersByVarType qualifiersByVarType,
                                                     final GroupContext groupContext,
                                                     final TranslationContext context) {
        final List<CodeBlock> results = new ArrayList<>();
        qualifiersByVarType.var.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), "", groupContext, context)));
        qualifiersByVarType.nonVar.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), "", groupContext, context)));
        return results;
    }

    /**
     * Creates a string representing a declared constraint
     */
    private CodeBlock topLevelConstraintBlock(final Expr expr, final String joinPredicateStr,
                                              @Nullable final GroupContext groupContext,
                                              final TranslationContext context) {
        Preconditions.checkArgument(expr instanceof BinaryOperatorPredicate);
        final String statement = maybeWrapped(expr, groupContext, context);

        if (joinPredicateStr.isEmpty()) {
            return CodeBlock.builder().addStatement("model.addEquality($L, 1)", statement).build();
        } else {
            return CodeBlock.builder().addStatement("model.addImplication($L, $L)", joinPredicateStr, statement)
                            .build();
        }
    }

    /**
     * Wrap constants 'x' in model.newConstant(x) depending on the type. Also converts true/false to 1/0.
     */
    private String maybeWrapped(final Expr expr,
                                @Nullable final GroupContext groupContext, final TranslationContext context) {
        String exprStr = exprToStr(expr, true, groupContext, context);

        // Some special cases to handle booleans because the or-tools API does not convert well to booleans
        if (expr instanceof Literal && ((Literal<?>) expr).getValue() instanceof Boolean) {
            exprStr = (Boolean) ((Literal<?>) expr).getValue() ? "1" : "0";
        }
        return tupleMetadata.inferType(expr) == JavaType.IntVar
                ? exprStr : String.format("o.toConst(%s)", exprStr);
    }

    /**
     * Creates array declarations and returns the set of tables that have controllable columns in them.
     */
    private void addArrayDeclarations(final MethodSpec.Builder output,
                                      final IRContext context) throws ClassNotFoundException {
        // For each table...
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable()) {
                continue;
            }

            // ...1) figure out the jooq record type (e.g., Record3<Integer, String, Boolean>)
            final Class<?> recordType = Class.forName("org.jooq.Record" + table.getIRColumns().size());
            final String recordTypeParameters = tupleMetadata.computeTableTupleType(table);

            if (table.isAliasedTable()) {
                continue;
            }
            output.addCode("\n");
            output.addCode("/* Table $S */\n", table.getName());
            // ...2) create a List<[RecordType]> to represent the table
            output.addStatement("final $T<$T<$L>> $L = (List<$T<$L>>) context.getTable($S).getCurrentData()",
                                 List.class, recordType, recordTypeParameters, tableNameStr(table.getName()),
                                 recordType, recordTypeParameters, table.getName());

            // ...3) create an index if configured
            if (configUseIndicesForEqualityBasedJoins) {
                // XXX: Generate this code only if an index is needed
                context.getTable(table.getName()).getPrimaryKey()
                        .map(IRPrimaryKey::getPrimaryKeyFields)
                        .filter(e -> !e.isEmpty())
                        .filter(e -> e.size() == 1)
                        .ifPresent(pkCol -> {
                            final int fieldIndex = tupleMetadata.getFieldIndexInTable(table.getName(),
                                                                                       pkCol.get(0).getName());
                            output.addStatement("final var $1LIndex = $2T.range(0, $3L.size())\n" +
                                        ".boxed()\n" +
                                        ".collect($4T.toMap(i -> ($7L) $3L.get(i).get($5L /* $6L */), i -> i));",
                                tableNameStr(table.getName()),
                                IntStream.class,
                                tableNameStr(table.getName()),
                                Collectors.class,
                                fieldIndex,
                                pkCol.get(0).getName(),
                                tupleMetadata.getTypeForField(table, pkCol.get(0)));
                            }
                    );
            }

            // ...4) for controllable fields, create a corresponding array of IntVars.
            for (final Map.Entry<String, IRColumn> fieldEntrySet : table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();
                if (field.isControllable()) {
                    final String variableName = fieldNameStr(table.getName(), fieldName);
                    output.addStatement("final $T[] $L = new $T[$L]", IntVar.class, variableName, IntVar.class,
                                                                       tableNumRowsStr(table.getName()))
                            .beginControlFlow("for (int i = 0; i < $L; i++)",
                                              tableNumRowsStr(table.getName()))
                            .addStatement("$L[i] = $N(model, $S)", variableName, INT_VAR_NO_BOUNDS, fieldName)
                            .endControlFlow();
                }
            }
        }

        // Once all tables and arrays can be referenced...
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            //..4) introduce primary-key constraints
            table.getPrimaryKey().ifPresent(e -> {
                if (!e.getPrimaryKeyFields().isEmpty() && e.hasControllableColumn()) {
                    output.addCode("\n");
                    output.addCode("/* Primary key constraints for $L */\n", tableNameStr(table.getName()));

                    // Use a specialized propagator if we only have a single column primary key
                    if (e.getPrimaryKeyFields().size() == 1) {
                        final IRColumn field = e.getPrimaryKeyFields().get(0);
                        final String fieldName = field.getName();
                        output.addStatement("model.addAllDifferent($L)",
                                fieldNameStr(table.getName(), fieldName));
                    }
                    else {
                        // We need to specify that tuples are unique. Perform this decomposition manually.
                        output.beginControlFlow("for (int i = 0; i < $L; i++)",
                                                 tableNumRowsStr(table.getName()));
                        output.beginControlFlow("for (int j = 0; i < j; j++)");

                        // non-controllable primary keys will be unique in the database,
                        // we don't need to add that as constraints in the solver
                        e.getPrimaryKeyFields().stream()
                            .filter(IRColumn::isControllable)
                            .forEach(field -> output.addStatement("model.addDifferent($L, $L)",
                                fieldNameStrWithIter(field.getIRTable().getName(), field.getName(), "i"),
                                fieldNameStrWithIter(field.getIRTable().getName(), field.getName(), "j"))
                        );
                        output.endControlFlow();
                        output.endControlFlow();
                    }
                }
            });

            //..5) introduce foreign-key constraints
            table.getForeignKeys().forEach(e -> {
                if (e.hasConstraint()) {
                    final AtomicInteger fieldIndex = new AtomicInteger(0);
                    e.getFields().forEach((child, parent) -> {
                        if (!child.isControllable()) {
                            return;
                        }
                        output.addCode("\n");
                        output.addCode("/* Foreign key constraints: $L.$L -> $L.$L */\n",
                                child.getIRTable().getName(), child.getName(),
                                parent.getIRTable().getName(), parent.getName());
                        output.beginControlFlow("for (int i = 0; i < $L; i++)",
                                tableNumRowsStr(table.getName()));
                        final String fkChildStr =
                                fieldNameStrWithIter(child.getIRTable().getName(), child.getName(), "i");
                        final String snippet = Joiner.on('\n').join(
                                "final long[] domain$L = context.getTable($S).getCurrentData()",
                                "                        .getValues($S, $L.class)",
                                "                        .stream()",
                                "                        .mapToLong(encoder::toLong).toArray()"
                        );
                        final JavaType parentType = InferType.typeFromColumn(parent);
                        output.addStatement(snippet,
                               fieldIndex.get(), parent.getIRTable().getName(), parent.getName().toUpperCase(Locale.US),
                               parentType.typeString());
                        output.addStatement("model.addLinearExpressionInDomain($L, $T.fromValues(domain$L))",
                                fkChildStr, Domain.class, fieldIndex.get());
                        output.endControlFlow();
                        fieldIndex.incrementAndGet();
                    });
                }
            });
        }

        output.addStatement(printTime("Array declarations"));
    }


    /**
     * Generates the initial statements within the generated solve() block
     */
    private void addInitializer(final MethodSpec.Builder output) {
        // ? extends Record
        final WildcardTypeName recordT = WildcardTypeName.subtypeOf(Record.class);
        // Result<? extends Record>
        final ParameterizedTypeName resultT = ParameterizedTypeName.get(ClassName.get(Result.class), recordT);
        final ClassName irTableT = ClassName.get(IRTable.class);
        final ClassName map = ClassName.get(Map.class);
        // Map<IRTable, Result<? extends Record>>
        final ParameterizedTypeName returnT = ParameterizedTypeName.get(map, irTableT, resultT);
        output.addModifiers(Modifier.PUBLIC)
               .returns(returnT)
               .addParameter(IRContext.class, "context", Modifier.FINAL)
               .addComment("Create the model.")
               .addStatement("final long startTime = $T.nanoTime()", System.class)
               .addStatement("final $T model = new $T()", CpModel.class, CpModel.class)
               .addStatement("final $1T encoder = new $1T()", StringEncoding.class)
               .addStatement("final $1T o = new $1T(model, encoder)", Ops.class)
               .addCode("\n");
    }

    /**
     * Generates the statements that create and configure the solver, before solving the created model
     */
    private void addSolvePhase(final MethodSpec.Builder output, final IRContext context) {
        output.addCode("\n")
               .addComment("Start solving")
               .addStatement(printTime("Model creation"))
               .addStatement("final $1T solver = new $1T()", CpSolver.class)
               .addStatement("solver.getParameters().setLogSearchProgress($L)", configPrintDiagnostics)
               .addStatement("solver.getParameters().setCpModelProbingLevel(0)")
               .addStatement("solver.getParameters().setNumSearchWorkers($L)", configNumThreads)
               .addStatement("solver.getParameters().setMaxTimeInSeconds($L)", configMaxTimeInSeconds)
               .addStatement("final $T status = solver.solve(model)", CpSolverStatus.class)
               .beginControlFlow("if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL)")
               .addStatement("final Map<IRTable, Result<? extends Record>> result = new $T<>()", HashMap.class)
               .addCode("final Object[] obj = new Object[1]; // Used to update controllable fields;\n");

        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            final String tableName = table.getName();
            // Only tables with variable columns become outputs
            if (table.hasVars()) {
                final String tempViewName = getTempViewName();
                output.addStatement("final Result<? extends Record> $L = " +
                        "context.getTable($S).getCurrentData()", tempViewName, tableName);

                table.getIRColumns().forEach(
                    (name, field) -> {
                        // Else, we update the records corresponding to the table.
                        if (field.isControllable()) {
                            output.beginControlFlow("for (int i = 0; i < $L; i++)",
                                    tableNumRowsStr(tableName));
                            if (field.getType().equals(IRColumn.FieldType.STRING)) {
                                output.addStatement("obj[0] = encoder.toStr(solver.value($L[i]))",
                                        fieldNameStr(tableName, field.getName()));
                            } else {
                                output.addStatement("obj[0] = solver.value($L[i])",
                                        fieldNameStr(tableName, field.getName()));
                            }
                            final int fieldIndex = tupleMetadata.getFieldIndexInTable(tableName, field.getName());
                            output.addStatement("$L.get(i).from(obj, $L)", tempViewName, fieldIndex);
                            output.endControlFlow();
                        }
                    }
                );
                output.addStatement("result.put(context.getTable($S), $L)", tableName, tempViewName);
            }
        }
        output.addStatement("return result");
        output.endControlFlow();
        output.addStatement("throw new $T(status.toString())", SolverException.class);
    }

    private static String tableNameStr(final String tableName) {
        return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName);
    }

    private static String fieldNameStr(final String tableName, final String fieldName) {
        return String.format("%s%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName),
                                     CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName));
    }

    private String fieldNameStrWithIter(final String tableName, final String fieldName, final String iterStr) {
        if (fieldName.contains("CONTROLLABLE")) {
            return String.format("%s[%s]", fieldNameStr(tableName, fieldName), iterStr);
        } else {
            if (tupleMetadata.canBeAccessedWithViewIndices(tableName)) {
                final int fieldIndex = tupleMetadata.getFieldIndexInView(tableName, fieldName);
                return String.format("%s.get(%s).value%s()", tableNameStr(tableName), iterStr, fieldIndex);
            } else {
                final JavaType type = tupleMetadata.getTypeForField(tableName, fieldName);
                final int fieldIndex = tupleMetadata.getFieldIndexInTable(tableName, fieldName);
                return String.format("(%s) %s.get(%s).get(%s /* %s */)",
                        type.typeString(), tableNameStr(tableName), iterStr, fieldIndex, fieldName);
            }
        }
    }

    private static String tableNumRowsStr(final String tableName) {
        return String.format("%s.size()", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private static String iterStr(final String tableName) {
        return String.format("%sIter", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private static String nonConstraintViewName(final String tableName) {
        return String.format("%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    // TODO: need to perform compilation entirely in memory
    private List<String> compile(final TypeSpec spec) {
        final JavaFile javaFile = JavaFile.builder("com.vmware.dcm.backend", spec).build();
        LOG.info("Generating Java or-tools code: {}\n", javaFile);

        // Compile Java code. This steps requires an SDK, and a JRE will not suffice
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        final Iterable<? extends JavaFileObject> compilationUnit = Collections.singleton(javaFile.toJavaFileObject());

        try {
            // Invoke SDK compiler
            final Path path = Path.of("/tmp/compilerOutput");
            final BufferedWriter fileWriter = Files.newBufferedWriter(path);
            final Boolean call = compiler.getTask(fileWriter, fileManager, null,
                    ImmutableList.of("-verbose", "-g", "-d", "/tmp/"), null, compilationUnit)
                    .call();
            if (!call) {
                final List<String> strings = Files.readAllLines(path);
                LOG.error("Compilation failed");
                for (final String line: strings) {
                    LOG.error("{}", line);
                }
                throw new RuntimeException();
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Once compiled, load the generated class, save an instance of it to the generatedBackend method
        // which we will invoke whenever we run the solver, and return the generated Java source to the caller.
        try {
            final File classesDir = new File(GENERATED_BACKEND_CLASS_FILE_PATH);
            // Load and instantiate compiled class.
            final URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()});
            // Loading the class
            final Class<?> cls = Class.forName(String.format("com.vmware.dcm.backend.%s", GENERATED_BACKEND_NAME), true,
                                               classLoader);
            final Constructor<?> declaredConstructor = cls.getDeclaredConstructor();
            generatedBackend = (IGeneratedBackend) declaredConstructor.newInstance();
            final StringWriter writer = new StringWriter();
            javaFile.writeTo(writer);
            return Collections.singletonList(writer.toString());
        } catch (final IOException | ClassNotFoundException | InstantiationException
                      | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> generateDataCode(final IRContext context) {
        this.context = context;
        return Collections.emptyList();
    }

    @Override
    public boolean needsGroupTables() {
        return false;
    }

    private String exprToStr(final Expr expr, final TranslationContext context) {
        return exprToStr(expr, true, null, context);
    }

    private String exprToStr(final Expr expr, final boolean allowControllable,
                             @Nullable final GroupContext currentGroup, final TranslationContext context) {
        final ExprToStrVisitor visitor = new ExprToStrVisitor(allowControllable, currentGroup, null);
        return visitor.visit(expr, context);
    }

    private ListComprehension rewritePipeline(final ListComprehension comprehension) {
        return Stream.of(comprehension)
                .map(RewriteArity::apply)
                .map(RewriteContains::apply)
                .findFirst().orElseThrow();
    }

    private QualifiersByVarType extractQualifiersByVarType(final ListComprehension comprehension,
                                                           final boolean skipAggregates) {
        final QualifiersByType varQualifiers = new QualifiersByType();
        final QualifiersByType nonVarQualifiers = new QualifiersByType();
        comprehension.getQualifiers().forEach(
            q -> {
                final GetVarQualifiers.QualifiersList qualifiersList = GetVarQualifiers.apply(q, skipAggregates);
                qualifiersList.getNonVarQualifiers().forEach(nonVarQualifiers::addQualifierByType);
                qualifiersList.getVarQualifiers().forEach(varQualifiers::addQualifierByType);
            }
        );
        Preconditions.checkArgument(varQualifiers.tableRowGenerators.isEmpty());
        return new QualifiersByVarType(varQualifiers, nonVarQualifiers);
    }

    private static class QualifiersByVarType {
        private final QualifiersByType var;
        private final QualifiersByType nonVar;

        public QualifiersByVarType(final QualifiersByType varQualifiers, final QualifiersByType nonVarQualifiers) {
            this.nonVar = nonVarQualifiers;
            this.var = varQualifiers;
        }
    }

    /**
     *
     */
    OutputIR.Block createCapacityConstraint(final String viewName, final ListComprehension comprehension,
                                            final TranslationContext context,
                                            final List<FunctionCall> capacityConstraint) {
        Preconditions.checkArgument(comprehension instanceof GroupByComprehension);
        final GroupByComprehension groupByComprehension = (GroupByComprehension) comprehension;
        final ListComprehension innerComprehension = groupByComprehension.getComprehension();
        final OutputIR.Block block = outputIR.newBlock(viewName);
        context.enterScope(block);

        // Add individual for loops to extract what we need. This would use table-row generators.
        final Map<String, OutputIR.ForBlock> tableToForBlock = new HashMap<>();
        final List<TableRowGenerator> tableRowGenerators =
                innerComprehension.getQualifiers().stream().filter(q -> q instanceof TableRowGenerator)
                             .map(q -> (TableRowGenerator) q).collect(Collectors.toList());
        tableRowGenerators.forEach(
                tableRowGenerator -> {
                    final CodeBlock codeBlock = forLoopFromTableRowGeneratorBlock(tableRowGenerator);
                    final OutputIR.ForBlock forBlock = outputIR.newForBlock("", codeBlock);
                    block.addBody(forBlock);
                    tableToForBlock.put(tableRowGenerator.getTable().getName(), forBlock);
                }
        );
        Preconditions.checkArgument(tableToForBlock.size() > 0);
        final Set<String> vars = new HashSet<>();
        final Set<String> domain = new HashSet<>();
        final List<String> demands = new ArrayList<>();
        final List<String> capacities = new ArrayList<>();

        capacityConstraint.forEach(
            functionCall -> {
                Preconditions.checkArgument(functionCall.getFunction()
                                                          .equals(FunctionCall.Function.CAPACITY_CONSTRAINT));
                final List<Expr> arguments = functionCall.getArgument();
                Preconditions.checkArgument(arguments.size() == 4);
                for (int i = 0; i < 4; i++) {
                    final Expr arg = arguments.get(i);
                    Preconditions.checkArgument(arg instanceof ColumnIdentifier);
                    final ColumnIdentifier columnArg = (ColumnIdentifier) arg;
                    final OutputIR.ForBlock forBlock = tableToForBlock.get(columnArg.getTableName());
                    context.enterScope(forBlock);
                    final String variableToAssignTo = exprToStr(columnArg, context);
                    final JavaType type = tupleMetadata.inferType(columnArg);
                    final boolean shouldCoerce = (i == 2 || i == 3) && type != JavaType.Long;
                    if (shouldCoerce) {
                        // if demands/capacities are in Integer domain, we coerce to longs
                        forBlock.addBody(statement("final long $L_l = (long) $L", variableToAssignTo,
                                                                                  variableToAssignTo));
                    }
                    context.leaveScope();
                    final String parameter = extractListFromLoop(shouldCoerce ?
                                                                    variableToAssignTo + "_l" : variableToAssignTo,
                                                                 context.currentScope(),
                                                                 forBlock,
                                                                 shouldCoerce ? JavaType.Long : type);

                    if (i == 0) { // vars
                        vars.add(parameter);
                    } else if (i == 1) { // domain
                        domain.add(parameter);
                    } else if (i == 2) { // demands
                        demands.add(parameter);
                    } else { // capacities
                        capacities.add(parameter);
                    }
                }
            }
        );
        context.leaveScope();
        Preconditions.checkArgument(vars.size() == 1 && domain.size() == 1);
        final String varsParameterStr = vars.iterator().next();
        final String domainParameterStr = domain.iterator().next();
        final String demandsParameterStr = String.join(", ", demands);
        final String capacitiesParameterStr = String.join(", ", capacities);
        block.addBody(CodeBlock.of("o.capacityConstraint($L, $L, $T.of($L), $T.of($L));",
                                  varsParameterStr, domainParameterStr,
                                  List.class, demandsParameterStr, List.class, capacitiesParameterStr));
        return block;
    }

    /**
     * The main logic to parse a comprehension and translate it into a set of intermediate variables and expressions.
     */
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE") // false positive
    private class ExprToStrVisitor extends IRVisitor<String, TranslationContext> {
        private final boolean allowControllable;
        @Nullable private final GroupContext currentGroupContext;
        @Nullable private final SubQueryContext currentSubQueryContext;

        private ExprToStrVisitor(final boolean allowControllable,
                                 @Nullable final GroupContext currentGroupContext,
                                 @Nullable final SubQueryContext currentSubQueryContext) {
            this.allowControllable = allowControllable;
            this.currentGroupContext = currentGroupContext;
            this.currentSubQueryContext = currentSubQueryContext;
        }

        @Override
        protected String visitFunctionCall(final FunctionCall node, final TranslationContext context) {
            final String vectorName = currentSubQueryContext == null ? currentGroupContext.getGroupViewName()
                                                                     : currentSubQueryContext.getSubQueryName();
            // Functions always apply on a vector. We compute the arguments to the function, and in doing so,
            // add declarations to the corresponding for-loop that extracts the relevant columns/expressions from views.
            final OutputIR.Block forLoop = context.currentScope().getForLoopByName(vectorName);

            // We have a special case for sums, because of an optimization where a sum of products can be better
            // represented as a scalar product in or-tools
            if (node.getFunction().equals(FunctionCall.Function.SUM)) {
                return attemptSumAsScalarProductOptimization(node.getArgument().get(0), context.currentScope(),
                                                         forLoop, context);
            }

            context.enterScope(forLoop);
            final String processedArgument = visit(node.getArgument().get(0), context.withEnterFunctionContext());
            context.leaveScope();

            final JavaType argumentType = tupleMetadata.inferType(node.getArgument().get(0));
            final boolean argumentIsIntVar = argumentType == JavaType.IntVar;

            final String listOfProcessedItem =
                    extractListFromLoop(processedArgument, context.currentScope(), forLoop, argumentType);
            final String function;
            switch (node.getFunction()) {
                case SUM:
                    throw new IllegalStateException("Unreachable");
                case COUNT:
                    // In these cases, it is safe to replace count(argument) with sum(1)
                    if ((node.getArgument().get(0) instanceof Literal ||
                         node.getArgument().get(0) instanceof ColumnIdentifier)) {
                        // TODO: another sign that groupContext/subQueryContext should be handled by ExprContext
                        //  and scopes
                        final String scanOver = currentSubQueryContext == null ?
                                                   "data" : currentSubQueryContext.getSubQueryName();
                        return apply(argumentIsIntVar
                                      ? CodeBlock.of("o.toConst($L.size())", scanOver).toString()
                                      : CodeBlock.of("$L.size()", scanOver).toString(), context);
                    }
                    function = argumentIsIntVar ? "sumV" : "sum";
                    break;
                case MAX:
                    function = String.format("maxV%s", argumentType);
                    break;
                case MIN:
                    function = String.format("minV%s", argumentType);
                    break;
                case ALL_EQUAL:
                    if (argumentIsIntVar) {
                        context.currentScope().addBody(statement("o.allEqualVar($L)", listOfProcessedItem));
                        return apply("model.newConstant(1)", context);
                    } else {
                        function = "allEqualPrimitive";
                        break;
                    }
                case ALL_DIFFERENT:
                    context.currentScope().addBody(statement("o.allDifferent($L)", listOfProcessedItem));
                    return apply("model.newConstant(1)", context);
                case INCREASING:
                    context.currentScope().addBody(statement("o.increasing($L)", listOfProcessedItem));
                    return apply("model.newConstant(1)", context);
                default:
                    throw new UnsupportedOperationException("Unsupported aggregate function " + node.getFunction());
            }
            return CodeBlock.of("o.$L($L)", function, listOfProcessedItem).toString();
        }

        @Override
        protected String visitExistsPredicate(final ExistsPredicate node, final TranslationContext context) {
            final String processedArgument = visit(node.getArgument(), context);
            return apply(String.format("o.exists(%s)", processedArgument), context);
        }

        @Override
        protected String visitIsNullPredicate(final IsNullPredicate node, final TranslationContext context) {
            final JavaType type = tupleMetadata.inferType(node.getArgument());
            final String processedArgument = visit(node.getArgument(), context);
            Preconditions.checkArgument(type != JavaType.IntVar);
            return apply(String.format("%s == null", processedArgument), context);
        }

        @Override
        protected String visitIsNotNullPredicate(final IsNotNullPredicate node, final TranslationContext context) {
            final JavaType type = tupleMetadata.inferType(node.getArgument());
            final String processedArgument = visit(node.getArgument(), context);
            Preconditions.checkArgument(type != JavaType.IntVar);
            return apply(String.format("%s != null", processedArgument), context);
        }

        @Override
        protected String visitUnaryOperator(final UnaryOperator node, final TranslationContext context) {
            switch (node.getOperator()) {
                case NOT:
                    return apply(String.format("o.not(%s)", visit(node.getArgument(), context)), context);
                case MINUS:
                    return apply(String.format("o.mult(-1, %s)", visit(node.getArgument(), context)), context);
                case PLUS:
                    return apply(visit(node.getArgument(), context), context);
                default:
                    throw new IllegalArgumentException(node.toString());
            }
        }

        @Override
        protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                      final TranslationContext context) {
            final String left = visit(node.getLeft(), context);
            final String right = visit(node.getRight(), context);
            final BinaryOperatorPredicate.Operator op = node.getOperator();
            final JavaType leftType = tupleMetadata.inferType(node.getLeft());
            final JavaType rightType = tupleMetadata.inferType(node.getRight());

            if (leftType == JavaType.IntVar || rightType == JavaType.IntVar) {
                // We need to generate an IntVar.
                switch (op) {
                    case EQUAL:
                        return apply(String.format("o.eq(%s, %s)", left, right), context);
                    case NOT_EQUAL:
                        return apply(String.format("o.ne(%s, %s)", left, right), context);
                    case AND:
                        return apply(String.format("o.and(%s, %s)", left, right), context);
                    case OR:
                        return apply(String.format("o.or(%s, %s)", left, right), context);
                    case LESS_THAN_OR_EQUAL:
                        return apply(String.format("o.leq(%s, %s)", left, right), context);
                    case LESS_THAN:
                        return apply(String.format("o.lt(%s, %s)", left, right), context);
                    case GREATER_THAN_OR_EQUAL:
                        return apply(String.format("o.geq(%s, %s)", left, right), context);
                    case GREATER_THAN:
                        return apply(String.format("o.gt(%s, %s)", left, right), context);
                    case ADD:
                        return apply(String.format("o.plus(%s, %s)", left, right), context);
                    case SUBTRACT:
                        return apply(String.format("o.minus(%s, %s)", left, right), context);
                    case MULTIPLY:
                        return apply(String.format("o.mult(%s, %s)", left, right), context);
                    case DIVIDE:
                        return apply(String.format("o.div(%s, %s)", left, right), context);
                    case IN:
                        return apply(String.format("o.in%s(%s, %s)", rightType, left, right), context);
                    case CONTAINS:
                        return apply(String.format("o.inObjectArr(%s, %s)", right, left), context);
                    default:
                        throw new UnsupportedOperationException("Operator " + op);
                }
            }
            else {
                // Both operands are non-var, so we generate an expression in Java.
                switch (op) {
                    case EQUAL:
                        return apply(String.format("(o.eq(%s, %s))", left, right), context);
                    case NOT_EQUAL:
                        return apply(String.format("(!o.eq(%s, %s))", left, right), context);
                    case AND:
                        return apply(String.format("(%s && %s)", left, right), context);
                    case OR:
                        return apply(String.format("(%s || %s)", left, right), context);
                    case CONTAINS:
                        return apply(String.format("o.in(%s, %s)", right, left), context);
                    case IN:
                        return apply(String.format("(o.in(%s, %s))", left, right), context);
                    case LESS_THAN_OR_EQUAL:
                        return apply(String.format("(%s <= %s)", left, right), context);
                    case LESS_THAN:
                        return apply(String.format("(%s < %s)", left, right), context);
                    case GREATER_THAN_OR_EQUAL:
                        return apply(String.format("(%s >= %s)", left, right), context);
                    case GREATER_THAN:
                        return apply(String.format("(%s > %s)", left, right), context);
                    case ADD:
                        return apply(String.format("(%s + %s)", left, right), context);
                    case SUBTRACT:
                        return apply(String.format("(%s - %s)", left, right), context);
                    case MULTIPLY:
                        return apply(String.format("(%s * %s)", left, right), context);
                    case DIVIDE:
                        return apply(String.format("(%s / %s)", left, right), context);
                    default:
                        throw new UnsupportedOperationException("Operator " + op);
                }
            }
        }

        @Override
        protected String visitColumnIdentifier(final ColumnIdentifier node, final TranslationContext context) {
            // If we are evaluating a group-by comprehension, then column accesses that happen outside the context
            // of an aggregation function must refer to the grouping column, not the inner tuple being iterated over.
            if (!context.isFunctionContext() && currentGroupContext != null) {
                final String tableName = node.getTableName();
                final String fieldName = node.getField().getName();
                int columnNumber = 0;
                for (final ColumnIdentifier ci: currentGroupContext.getQualifier().getGroupByColumnIdentifiers()) {
                    if (ci.getTableName().equalsIgnoreCase(tableName)
                            && ci.getField().getName().equalsIgnoreCase(fieldName)) {
                        return apply(String.format("group.value%s()", columnNumber), context);
                    }
                    columnNumber++;
                }
                throw new UnsupportedOperationException("Could not find group-by column " + node);
            }

            // TODO: The next two blocks can both simultaneously be true. This can happen when we are within
            //  a subquery and a group-by context at the same time. We need a cleaner way to distinguish what
            //  scope to be searching for the indices.
            // Sub-queries also use an intermediate view, and we again need an indirection from column names to indices
            if (context.isFunctionContext() && currentSubQueryContext != null) {
                final String tempTableName = currentSubQueryContext.getSubQueryName().toUpperCase(Locale.US);
                final int fieldIndex = tupleMetadata.getFieldIndexInView(tempTableName, node.getField().getName());
                return apply(String.format("%s.value%s()", context.getTupleVarName(), fieldIndex), context);
            }

            // Within a group-by, we refer to values from the intermediate group by table. This involves an
            // indirection from columns to tuple indices
            if (context.isFunctionContext() && currentGroupContext != null) {
                final String tempTableName = currentGroupContext.getTempTableName().toUpperCase(Locale.US);
                final int fieldIndex = tupleMetadata.getFieldIndexInView(tempTableName, node.getField().getName());
                return apply(String.format("dataTuple.value%s()", fieldIndex), context);
            }

            final String tableName = node.getField().getIRTable().getName();
            final String fieldName = node.getField().getName();
            final String iterStr = iterStr(node.getTableName());
            if (!allowControllable && fieldName.contains("CONTROLLABLE")) {
                throw new UnsupportedOperationException("Controllable variables not allowed in predicates");
            }
            return apply(fieldNameStrWithIter(tableName, fieldName, iterStr), context);
        }

        @Override
        protected String visitLiteral(final Literal node, final TranslationContext context) {
            if (node.getValue() instanceof String) {
                return node.getValue().toString().replace("'", "\"");
            } else {
                return node.getValue().toString();
            }
        }

        @Override
        protected String visitListComprehension(final ListComprehension node, final TranslationContext context) {
            // We are in a subquery.
            final String newSubqueryName = context.getNewSubqueryName();
            final OutputIR.Block subQueryBlock = viewBlock(newSubqueryName, node, false, context);
            Preconditions.checkArgument(node.getHead().getSelectExprs().size() == 1);
            final ExprToStrVisitor innerVisitor =
                    new ExprToStrVisitor(allowControllable, currentGroupContext,
                                         new SubQueryContext(newSubqueryName));
            Preconditions.checkArgument(node.getHead().getSelectExprs().size() == 1);
            final Expr headSelectItem = node.getHead().getSelectExprs().get(0);

            final ContainsFunctionCall visitor = new ContainsFunctionCall();
            visitor.visit(headSelectItem);
            final boolean headSelectItemContainsMonoidFunction = visitor.getFound();

            attemptConstantSubqueryOptimization(node, subQueryBlock, context);

            final TranslationContext newCtx = context.withEnterFunctionContext();
            // If the head contains a function, then this is a scalar subquery
            if (headSelectItemContainsMonoidFunction) {
                newCtx.enterScope(subQueryBlock);
                final String ret = apply(innerVisitor.visit(headSelectItem, newCtx), context);
                newCtx.leaveScope();
                return ret;
            } else {
                // Else, treat the result as a vector
                newCtx.enterScope(subQueryBlock.getForLoopByName(newSubqueryName));
                final String processedHeadItem = innerVisitor.visit(node.getHead().getSelectExprs().get(0), newCtx);
                final JavaType type = tupleMetadata.inferType(node.getHead().getSelectExprs().get(0));
                final String listName =
                        extractListFromLoop(processedHeadItem, subQueryBlock, newSubqueryName, type);
                newCtx.leaveScope();
                return apply(listName, subQueryBlock, context);
            }
        }

        @Override
        protected String visitGroupByComprehension(final GroupByComprehension node, final TranslationContext context) {
            // We are in a subquery.
            final String newSubqueryName = context.getNewSubqueryName();
            final OutputIR.Block subQueryBlock = viewBlock(newSubqueryName, node, false, context);
            Preconditions.checkArgument(node.getComprehension().getHead().getSelectExprs().size() == 1);
            final ExprToStrVisitor innerVisitor = new ExprToStrVisitor(allowControllable, currentGroupContext,
                                                                       new SubQueryContext(newSubqueryName));
            Preconditions.checkArgument(node.getComprehension().getHead().getSelectExprs().size() == 1);
            final Expr headSelectItem = node.getComprehension().getHead().getSelectExprs().get(0);

            final TranslationContext newCtx = context.withEnterFunctionContext();

            attemptConstantSubqueryOptimization(node, subQueryBlock, context);

            // if scalar subquery
            if (headSelectItem instanceof FunctionCall) {
                newCtx.enterScope(subQueryBlock);
                final String ret = apply(innerVisitor.visit(headSelectItem, newCtx), context);
                newCtx.leaveScope();
                return ret;
            } else {
                newCtx.enterScope(subQueryBlock.getForLoopByName(newSubqueryName));
                final String processedHeadItem =
                    innerVisitor.visit(node.getComprehension().getHead().getSelectExprs().get(0), newCtx);
                final JavaType type =
                        tupleMetadata.inferType(node.getComprehension().getHead().getSelectExprs().get(0));
                final String listName =
                        extractListFromLoop(processedHeadItem, subQueryBlock, newSubqueryName, type);
                newCtx.leaveScope();
                // Treat as a vector
                return apply(listName, subQueryBlock, context);
            }
        }

        /**
         * Takes an expression, and declares it as an intermediate variable in the current context. It then
         * returns the declared variable name.
         *
         * @param expression expression to assign to a variable
         * @param context current context for translation
         * @return An intermediate variable name
         */
        protected String apply(final String expression, final TranslationContext context) {
            return context.declareVariable(expression);
        }

        /**
         * Takes an expression, and declares it as an intermediate variable within the supplied IR block. It then
         * returns the declared variable name.
         *
         * @param expression expression to assign to a variable
         * @param block the current block within which we want to declare the expression
         * @param context current context for translation
         * @return An intermediate variable name
         */
        protected String apply(final String expression, final OutputIR.Block block, final TranslationContext context) {
            return context.declareVariable(expression, block);
        }

        /**
         * A sum of an expression of the form (X * Y), where X is a variable and Y is a constant, is best
         * expressed as a scalar product which takes a list of variables, and a corresponding list of coefficients.
         * This is common in bin-packing problems where a list of variables corresponding to whether a task is
         * assigned to a bin, is weighted by the demands of each task, and the resulting weighted sum of variables
         * is used in capacity constraints.
         *
         * This optimization saves the CP-SAT solver a lot of effort in the pre-solving and solving phases,
         * leading to significant performance improvements.
         *
         * If we cannot perform this optimization, we revert to computing a sum just like any other function.
         * See visitMonoidFunction().
         *
         * @param node the argument of a sum() function
         * @param outerBlock the block within which the sum is being computed
         * @param forLoop the for loop block within which the arguments for the sum are extracted
         * @param context the current translation context
         * @return A variable that yields the result of the sum
         */
        private String attemptSumAsScalarProductOptimization(final Expr node, final OutputIR.Block outerBlock,
                                                             final OutputIR.Block forLoop,
                                                             final TranslationContext context) {
            if (configTryScalarProductEncoding && node instanceof BinaryOperatorPredicate) {
                final BinaryOperatorPredicate operation = (BinaryOperatorPredicate) node;
                final BinaryOperatorPredicate.Operator op = operation.getOperator();
                final Expr left = operation.getLeft();
                final Expr right = operation.getRight();
                final JavaType leftType = tupleMetadata.inferType(left);
                final JavaType rightType = tupleMetadata.inferType(right);
                // TODO: The multiply may not necessarily be the top level operation.
                if (op.equals(BinaryOperatorPredicate.Operator.MULTIPLY)) {
                    if (leftType == JavaType.IntVar && rightType != JavaType.IntVar) {
                        return createTermsForScalarProduct(left, right, context, outerBlock, forLoop, rightType);
                    }
                    if (rightType == JavaType.IntVar && leftType != JavaType.IntVar) {
                        return createTermsForScalarProduct(right, left, context, outerBlock, forLoop, leftType);
                    }
                }
            }
            // regular sum
            context.enterScope(forLoop);
            final String processedArgument = visit(node, context.withEnterFunctionContext());
            context.leaveScope();
            final JavaType argumentType = tupleMetadata.inferType(node);
            final String function = argumentType == JavaType.IntVar ? "sumV" :
                                    argumentType  == JavaType.Long ? "sumLong" :
                                     "sumInteger";
            final String listOfProcessedItem =
                    extractListFromLoop(processedArgument, context.currentScope(), forLoop, argumentType);
            return CodeBlock.of("o.$L($L)", function, listOfProcessedItem).toString();
        }

        /**
         * Given two expressions representing a variable and coefficients, generate a scalar product term. To do so,
         * we extract the necessary lists from the for loop where these variables and coefficients are collected.
         */
        private String createTermsForScalarProduct(final Expr variables, final Expr coefficients,
                                                   final TranslationContext context, final OutputIR.Block outerBlock,
                                                   final OutputIR.Block forLoop,
                                                   final JavaType coefficientsType) {
            context.enterScope(forLoop);
            final String variablesItem = visit(variables, context.withEnterFunctionContext());
            final String coefficientsItem = visit(coefficients, context.withEnterFunctionContext());
            context.leaveScope();
            final String listOfVariablesItem =
                    extractListFromLoop(variablesItem, outerBlock, forLoop, JavaType.IntVar);
            final String listOfCoefficientsItem =
                    extractListFromLoop(coefficientsItem, outerBlock, forLoop, coefficientsType);
            return CodeBlock.of("o.scalProd$L($L, $L)", coefficientsType, listOfVariablesItem, listOfCoefficientsItem)
                            .toString();
        }

        /**
         * Constant sub-queries can be floated to the root block so that we evaluate them only once
         */
        private void attemptConstantSubqueryOptimization(final ListComprehension node,
                                                         final OutputIR.Block subQueryBlock,
                                                         final TranslationContext context) {
            if (IsConstantSubquery.apply(node)) {
                final OutputIR.Block rootBlock = context.getRootBlock();
                rootBlock.addBody(subQueryBlock);
            } else {
                context.currentScope().addBody(subQueryBlock);
            }
        }
    }

    /**
     * Used to extract a variable computed within each iteration of a loop into a list for later use (for
     * example, aggregate functions).
     *
     * @param variableToExtract a variable name from within a block to extract out of a loop
     * @param outerBlock the block outside the loop where the extracted list will be created
     * @param innerBlock the name of the block from which we want to extract a list of `variableToExtract` instances
     * @param variableType the type of `variableToExtract`
     * @return the name of the list being extracted
     */
    private String extractListFromLoop(final String variableToExtract, final OutputIR.Block outerBlock,
                                       final OutputIR.Block innerBlock, final JavaType variableType) {
        final String listName = "listOf" + variableToExtract;
        // For computing aggregates, the list being scanned is always named "data", and
        // those loop blocks have a valid size. Use this for pre-allocating lists.
        final String maybeGuessSize = innerBlock instanceof OutputIR.ForBlock ?
                ((OutputIR.ForBlock) innerBlock).getSize() : "";
        final boolean wasAdded = outerBlock.addHeader(statement("final List<$L> listOf$L = new $T<>($L)",
                variableType, variableToExtract, ArrayList.class, maybeGuessSize));
        if (wasAdded) {
            innerBlock.addBody(statement("$L.add($L)", listName, variableToExtract));
        }
        return listName;
    }

    /**
     * Used to extract a variable computed within each iteration of a loop into a list for later use (for
     * example, aggregate functions).
     *
     * @param variableToExtract a variable name from within a block to extract out of a loop
     * @param outerBlock the block outside the loop where the extracted list will be created
     * @param loopBlockName the name of the loop from which we want to extract a list
     * @param variableType the type of `variableToExtract`
     * @return the name of the list being extracted
     */
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // false positive
    private String extractListFromLoop(final String variableToExtract, final OutputIR.Block outerBlock,
                                       final String loopBlockName, final JavaType variableType) {
        final OutputIR.Block forLoop = outerBlock.getForLoopByName(loopBlockName);
        return extractListFromLoop(variableToExtract, outerBlock, forLoop, variableType);
    }

    private static class QualifiersByType {
        private final List<BinaryOperatorPredicate> wherePredicates = new ArrayList<>();
        private final List<JoinPredicate> joinPredicates = new ArrayList<>();
        private final List<BinaryOperatorPredicateWithAggregate> aggregatePredicates = new ArrayList<>();
        private final List<TableRowGenerator> tableRowGenerators = new ArrayList<>();
        private final List<CheckQualifier> checkQualifiers = new ArrayList<>();

        private void addQualifierByType(final Qualifier q) {
            if (q instanceof CheckQualifier) {
                checkQualifiers.add((CheckQualifier) q);
            } else if (q instanceof BinaryOperatorPredicateWithAggregate) {
                aggregatePredicates.add((BinaryOperatorPredicateWithAggregate) q);
            } else if (q instanceof JoinPredicate) {
                joinPredicates.add((JoinPredicate) q);
            } else if (q instanceof BinaryOperatorPredicate) {
                wherePredicates.add((BinaryOperatorPredicate) q);
            } else if (q instanceof TableRowGenerator) {
                tableRowGenerators.add((TableRowGenerator) q);
            } else if (q instanceof GroupByQualifier) {
                System.err.println("Ignoring group-by qualifier");
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public String toString() {
            return String.format("{aggregatePredicates: %s, joinPredicates: %s, wherePredicates: %s}",
                                  aggregatePredicates, joinPredicates, wherePredicates);
        }
    }

    private CodeBlock printTime(final String event) {
        if (configPrintDiagnostics) {
            return CodeBlock.of("System.out.println(\"$L: we are at \" + (System.nanoTime() - startTime));", event);
        } else {
            return CodeBlock.of("");
        }
    }

    private String getTempViewName() {
        return  "tmp" + intermediateViewCounter.getAndIncrement();
    }

    private static CodeBlock statement(final String format, final Object... args) {
        return CodeBlock.builder().addStatement(format, args).build();
    }
}