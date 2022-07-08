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
import com.google.ortools.Loader;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.IntVar;
import com.google.ortools.util.Domain;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import com.vmware.dcm.backend.IGeneratedBackend;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.backend.RewriteArity;
import com.vmware.dcm.backend.RewriteContains;
import com.vmware.dcm.compiler.IRColumn;
import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.compiler.IRTable;
import com.vmware.dcm.compiler.Program;
import com.vmware.dcm.compiler.UsesControllableFields;
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
    private static final String INPUT_DATA_VARIABLE = "INPUT_DATA";
    private static final Logger LOG = LoggerFactory.getLogger(OrToolsSolver.class);
    private static final String GENERATED_BACKEND_NAME = "GeneratedBackend";
    private final AtomicInteger intermediateViewCounter = new AtomicInteger(0);
    private final TupleMetadata tupleMetadata = new TupleMetadata();
    private final TupleGen tupleGen = new TupleGen();
    private final OutputIR outputIR = new OutputIR();
    private final int configNumThreads;
    private final int configMaxTimeInSeconds;
    private final boolean configTryScalarProductEncoding;
    private final boolean configUseIndicesForEqualityBasedJoins;
    private final boolean configPrintDiagnostics;
    private final boolean configUseCapacityPresenceLiterals;

    static {
        Loader.loadNativeLibraries();
    }

    @Nullable private IGeneratedBackend generatedBackend;

    private OrToolsSolver(final int configNumThreads, final int configMaxTimeInSeconds,
                          final boolean configTryScalarProductEncoding,
                          final boolean configUseIndicesForEqualityBasedJoins,
                          final boolean configPrintDiagnostics,
                          final boolean configUseCapacityPresenceLiterals) {
        this.configNumThreads = configNumThreads;
        this.configMaxTimeInSeconds = configMaxTimeInSeconds;
        this.configTryScalarProductEncoding = configTryScalarProductEncoding;
        this.configUseIndicesForEqualityBasedJoins = configUseIndicesForEqualityBasedJoins;
        this.configPrintDiagnostics = configPrintDiagnostics;
        this.configUseCapacityPresenceLiterals = configUseCapacityPresenceLiterals;
    }

    public static class Builder {
        private int numThreads = NUM_THREADS_DEFAULT;
        private int maxTimeInSeconds = MAX_TIME_IN_SECONDS;
        private boolean tryScalarProductEncoding = true;
        private boolean useIndicesForEqualityBasedJoins = true;
        private boolean printDiagnostics = false;
        private boolean useCapacityPresenceLiterals = true;

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

        /**
         * Configures whether or not to use presence literals in the capacity constraint function. If true,
         * a capacity constraint is allowed to fail (that is, it is possible that some elements cannot be
         * assigned to their domains in a way that satisfy capacity constraints).
         *
         * @param useCapacityPresenceLiterals use presence literals in build-in capacity constraint function. Defaults
         *                                    to true.
         * @return the current Builder object with `useCapacityPresenceLiterals` set
         */
        public Builder setUseCapacityPresenceLiterals(final boolean useCapacityPresenceLiterals) {
            this.useCapacityPresenceLiterals = useCapacityPresenceLiterals;
            return this;
        }

        public OrToolsSolver build() {
            return new OrToolsSolver(numThreads, maxTimeInSeconds, tryScalarProductEncoding,
                                     useIndicesForEqualityBasedJoins, printDiagnostics, useCapacityPresenceLiterals);
        }
    }

    @Override
    public Map<String, Result<? extends Record>> runSolver(final Map<String, Result<? extends Record>> inputRecords) {
        Preconditions.checkNotNull(generatedBackend);
        return generatedBackend.solve(inputRecords);
    }

    /**
     * This method is where the code generation happens.
     *
     * It generates an instance of IGeneratedBackend with a solve() method. The solve method
     * creates all the IntVar instances required for variable columns, and translates comprehensions
     * into nested for loops. It re-uses JOOQ tables wherever possible for constants.
     */
    @Override
    public List<String> generateModelCode(final IRContext context, final Program<ListComprehension> program) {
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

        final var translationContext = new TranslationContext(false);
        final var determineIndexes = new DetermineIndexes();
        // Lower the program to a block of Java code.
        // Start with some passes on Program<ListComprehension>
        final Program<ListComprehension> passOne = program
                .map(RewriteArity::apply)
                .map(RewriteContains::apply)
                .map(l -> configTryScalarProductEncoding ? ScalarProductOptimization.apply(l, tupleMetadata) : l)
                .peek((n, l) -> determineIndexes.apply(l));
        addIndexes(determineIndexes, output);

        // ... then convert to Program<OutputIR.Block>
        passOne.map(
                (name, comprehension) -> viewCodeGen(name, comprehension,
                                                     ConstraintType.NON_CONSTRAINT, translationContext),
                (name, comprehension) -> viewCodeGen(name, comprehension,
                                                     ConstraintType.HARD_CONSTRAINT, translationContext),
                (name, comprehension) -> viewCodeGen(name, comprehension,
                                                     ConstraintType.OBJECTIVE, translationContext)
               )
               // ... then lower to Program<CodeBlock>
               .map(outputIrBlock -> CodeBlock.builder().add(outputIrBlock.toString()).build())
               // ... then add the resulting string to the generated method
               .forEach((name, codeBlock) -> output.addCode(codeBlock));
        addSolvePhase(output, context);
        final MethodSpec solveMethod = output.build();

        final TypeSpec.Builder backendClassBuilder = TypeSpec.classBuilder(GENERATED_BACKEND_NAME)
                .addAnnotation(AnnotationSpec.builder(Generated.class)
                                 .addMember("value", "$S", "com.vmware.dcm.backend.ortools.OrToolsSolver")
                                 .build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(IGeneratedBackend.class)
                .addMethod(solveMethod);
        tupleGen.getAllTupleTypes().forEach(backendClassBuilder::addType); // Add tuple types

        final TypeSpec spec = backendClassBuilder.build();
        return compile(spec);
    }

    private void addIndexes(final DetermineIndexes determineIndexes, final MethodSpec.Builder output) {
        if (configUseIndicesForEqualityBasedJoins) {
            final Set<String> createdIndices = new HashSet<>();
            determineIndexes.indexes().forEach(
                id -> {
                    final IRTable table = id.relation;
                    final IRColumn column = id.columnBeingAccessed.getField();
                    final String indexName = indexName(id);
                    // Indexed accesses can happen for the same column under different aliases.
                    // We disambiguate here.
                    if (!createdIndices.contains(indexName)) {
                        final int fieldIndex = tupleMetadata.getFieldIndexInTable(table.getName(), column.getName());
                        output.addStatement("final var $1L = o.toIndex($2L, (r) -> r.get($3L /* $4L */, $5L.class))",
                                indexName, tableNameStr(table.getName()), fieldIndex, column.getName(),
                                tupleMetadata.getTypeForField(table, column));
                        createdIndices.add(indexName);
                    }
                }
            );
        }
    }

    private OutputIR.Block viewCodeGen(final String name, final ListComprehension comprehension,
                                       final ConstraintType type, final TranslationContext translationContext) {
        final OutputIR.Block outerBlock = outputIR.newBlock("outer");
        translationContext.enterScope(outerBlock);
        final OutputIR.Block block = viewBlock(name, comprehension, type, translationContext);
        outerBlock.addBody(block);
        return outerBlock;
    }

    /**
     * Creates and populates a block, representing a view. This corresponds to one or more sets of nested
     * for loops and result sets.
     * @param viewName name of the view to be created
     * @param comprehension comprehension corresponding to `viewName`, to be translated into a block of code
     * @param constraintType whether the comprehension represents a constraint
     * @param context the translation context for the comprehension
     * @return the block created for the comprehension
     */
    private OutputIR.Block viewBlock(final String viewName, final ListComprehension comprehension,
                                     final ConstraintType constraintType, final TranslationContext context) {
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
                    innerComprehensionBlock(intermediateView, inner, groupByQualifier, constraintType, context);
            block.addBody(intermediateViewBlock);

            // We now construct the actual result set that hosts the aggregated tuples by group. This is done
            // in two steps...

            // (1) Create the result set
            block.addBody(CodeBlock.builder()
                                 .add("\n")
                                 .addStatement(printTime("Group-by intermediate view"))
                                 .add("/* $L view $L */\n", constraintType, tableNameStr(viewName))
                                 .build()
            );

            if (constraintType == ConstraintType.NON_CONSTRAINT) {
                final int selectExprSize = inner.getHead().getSelectExprs().size();
                final TypeSpec typeSpec = tupleGen.getTupleType(selectExprSize);
                final JavaTypeList viewTupleGenericParameters =
                        tupleMetadata.recordViewTupleType(viewName, inner.getHead().getSelectExprs());

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

            final TranslationContext groupByContext = context.withEnterGroupContext(groupByQualifier, intermediateView,
                    viewName);
            groupByContext.enterScope(forBlock);

            final String groupName = groupByContext.getGroupContext().getGroupName();
            final String groupDataName = groupByContext.getGroupContext().getGroupDataName();
            final String groupDataTupleName = groupByContext.getGroupContext().getGroupDataTupleName();

            forBlock.addHeader(CodeBlock.builder().addStatement("final var $L = entry.getKey()", groupName)
                                                  .addStatement("final var $L = entry.getValue()", groupDataName)
                                                  .build());

            final JoinStrategy.Type joinStrategy = JoinStrategy.apply(inner);
            final CodeBlock forExpr = joinStrategy == JoinStrategy.Type.MATERIALIZED ?
                    CodeBlock.of("for (final var $L: $L)", groupDataTupleName, groupDataName) :
                    CodeBlock.of("/* Intentionally empty to handle unmaterialized join */");
            final OutputIR.ForBlock dataForBlock = outputIR.newForBlock(viewName, forExpr, groupDataName + ".size()");
            forBlock.addBody(dataForBlock);

            // (3) Filter if necessary
            final QualifiersByVarType qualifiersByVarType = extractQualifiersByVarType(inner);
            final Optional<OutputIR.IfBlock> nonVarAggregateFiltersBlock = nonVarAggregateFiltersBlock(viewName,
                    qualifiersByVarType.nonVar, groupByContext);
            nonVarAggregateFiltersBlock.ifPresent(forBlock::addBody);

            // If this is not a constraint, we simply add a to a result set
            if (constraintType == ConstraintType.NON_CONSTRAINT) {
                final TypeSpec typeSpec = tupleGen.getTupleType(inner.getHead().getSelectExprs().size());
                final String tupleResult = inner.getHead().getSelectExprs().stream()
                                                 .map(e -> toJavaExpression(e, groupByContext))
                                                 .map(JavaExpression::asString)
                                                 .collect(Collectors.joining(", "));
                forBlock.addBody(statement("final var $L = new $N<>($L)",
                                            groupByContext.getTupleVarName(), typeSpec, tupleResult));
                forBlock.addBody(statement("$L.add($L)", tableNameStr(viewName), groupByContext.getTupleVarName()));
            }
            else  {
                // If this is a constraint, we translate having clauses into a constraint statement
                final List<CodeBlock> constraintBlocks = aggregateConstraintBlock(qualifiersByVarType,
                                                                                  constraintType, groupByContext);
                constraintBlocks.forEach(forBlock::addBody);
            }
            groupByContext.leaveScope(); // forBlock
            block.addBody(forBlock);
            block.addBody(printTime("Group-by final view"));
            return block;
        }
        return innerComprehensionBlock(viewName, comprehension, null, constraintType, context);
    }

    /**
     * Converts a comprehension into a set of nested for loops that return a "result set". The result set
     * is represented as a list of tuples.
     */
    private OutputIR.Block innerComprehensionBlock(final String viewName,
                                                   final ListComprehension comprehension,
                                                   @Nullable final GroupByQualifier groupByQualifier,
                                                   final ConstraintType constraintType,
                                                   final TranslationContext context) {
        final OutputIR.Block viewBlock = outputIR.newBlock(viewName);
        // Add a comment with the view name
        viewBlock.addHeader(CodeBlock.builder().add("\n")
                     .add("/* $L view $L */\n", constraintType, viewName)
                     .build()
        );
        final JoinStrategy.Type joinType = JoinStrategy.apply(comprehension);

        // Extract the set of columns being selected in this view
        final List<Expr> headItemsList = getColumnsAccessed(comprehension, constraintType);
        context.enterScope(viewBlock);

        // For non-constraints, create a Map<> or a List<> to collect the result-set (depending on
        // whether the query is a group by or not)
        final OutputIR.Block resultSetDeclBlock = mapOrListForResultSetBlock(viewName, headItemsList,
                                                                             groupByQualifier, constraintType,
                                                                             joinType);
        viewBlock.addBody(resultSetDeclBlock);

        // Separate out qualifiers into variable and non-variable types.
        final QualifiersByVarType qualifiersByVarType = extractQualifiersByVarType(comprehension);

        // Start control flows to iterate over tables/views
        final OutputIR.ForBlock iterationBlock = tableIterationBlock(viewName, qualifiersByVarType.nonVar, context);
        viewBlock.addBody(iterationBlock);
        context.enterScope(iterationBlock);

        // Filter out nested for loops using an if(predicate) statement
        final Optional<OutputIR.IfBlock> nonVarFiltersBlock = nonVarFiltersBlock(viewName, qualifiersByVarType.nonVar,
                                                                                 context);
        nonVarFiltersBlock.ifPresent(iterationBlock::addBody);
        if (constraintType == ConstraintType.NON_CONSTRAINT
           || (groupByQualifier != null) // for aggregate constraints, we populate a
                                         // result set and post constraints elsewhere
        ) {
            // If filter predicate is true, then retrieve expressions to collect into result set. Note, this does not
            // evaluate things like functions (sum etc.). These are not aggregated as part of the inner expressions.
            final OutputIR.Block resultSetAddBlock = joinType == JoinStrategy.Type.MATERIALIZED ?
                    resultSetAddBlock(viewName, headItemsList, groupByQualifier, context) :
                    resultSetAddBlock(viewName, comprehension, groupByQualifier, context);
            iterationBlock.addBody(resultSetAddBlock);
        } else {
            final List<CodeBlock> addRowConstraintBlock =
                    rowConstraintBlock(qualifiersByVarType, constraintType, context);
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
    private List<Expr> getColumnsAccessed(final ListComprehension comprehension,
                                          final ConstraintType isConstraint) {
        if (isConstraint != ConstraintType.NON_CONSTRAINT) {
            final List<Expr> columnsAccessed = getColumnsAccessed(comprehension.getQualifiers());
            if (columnsAccessed.isEmpty()) {
                // There are constraints that are trivially true or false, wherein the predicate does not depend on
                // on any columns from the relations in the query. See the ModelTest.innerSubqueryCountTest
                // as an example. In such cases, we simply revert to pulling out all the head items for the outer query.
                return getColumnsAccessed(comprehension.getHead().getSelectExprs());
            }
            return columnsAccessed;
        } else {
            final List<Expr> columnsFromQualifiers = getColumnsAccessed(comprehension.getQualifiers());
            final List<Expr> columnsFromHead = getColumnsAccessed(comprehension.getHead().getSelectExprs());
            return Lists.newArrayList(Iterables.concat(columnsFromHead, columnsFromQualifiers));
        }
    }

    private List<Expr> getColumnsAccessed(final List<? extends Expr> exprs) {
        return exprs.stream()
                .map(this::getColumnsAccessed)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    private LinkedHashSet<Expr> getColumnsAccessed(final Expr expr) {
        final GetColumnIdentifiers visitor = new GetColumnIdentifiers(false);
        visitor.visit(expr);
        return visitor.getColumnIdentifiers();
    }

    /**
     * If required, returns a block of code representing maps or lists created to host the result-sets returned
     * by a view.
     */
    private OutputIR.Block mapOrListForResultSetBlock(final String viewName, final List<Expr> headItemsList,
                                                      @Nullable final GroupByQualifier groupByQualifier,
                                                      final ConstraintType isConstraint,
                                                      final JoinStrategy.Type joinStrategy) {
        final OutputIR.Block block = outputIR.newBlock(viewName + "CreateResultSet");

        // Compute a string that represents the Java types corresponding to the headItemsStr
        final JavaTypeList headItemsListTupleGenericParameters =
                tupleMetadata.recordViewTupleType(viewName, headItemsList);

        final TypeSpec tupleSpec = tupleGen.getTupleType(headItemsList.size());
        final String resultSetNameStr = nonConstraintViewName(viewName);
        final CodeBlock resultSetType = joinStrategy == JoinStrategy.Type.MATERIALIZED ?
                          CodeBlock.of("$T<$N<$L>>", List.class, tupleSpec, headItemsListTupleGenericParameters) :
                          CodeBlock.of("$T<$T, $T<$T>>", Map.class, String.class, Set.class, Integer.class);

        if (groupByQualifier != null) {
            // Create group by tuple
            final int numberOfGroupColumns = groupByQualifier.getGroupByExprs().size();
            final JavaTypeList groupByTupleGenericParameters =
                    tupleMetadata.computeTupleGenericParameters(groupByQualifier.getGroupByExprs());
            final TypeSpec groupTupleSpec = tupleGen.getTupleType(numberOfGroupColumns);

            block.addHeader(statement("final Map<$N<$L>, $L> $L = new $T<>()",
                                        groupTupleSpec,
                                        groupByTupleGenericParameters,
                                        resultSetType.toString(),
                                        resultSetNameStr, HashMap.class));
        } else {
            if (isConstraint == ConstraintType.NON_CONSTRAINT) {
                block.addHeader(statement("final $L $L = new $T<>()",
                                         resultSetType.toString(), resultSetNameStr, ArrayList.class));
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
    private OutputIR.ForBlock tableIterationBlock(final String viewName, final QualifiersByType nonVarQualifiers,
                                                  final TranslationContext context) {
        final List<CodeBlock> loopStatements =
                context.isSubQueryContext() ? forLoopsOrIndicesFromTableRowGenerators(
                                                        nonVarQualifiers.tableRowGenerators,
                                                        nonVarQualifiers.joinPredicates)
                                            : subQueryAccess(nonVarQualifiers);
        return outputIR.newForBlock(viewName, loopStatements);
    }

    private List<CodeBlock> subQueryAccess(final QualifiersByType nonVarQualifiers) {
        // Check if this is a correlated sub-query that can be expressed as an equijoin
        if (nonVarQualifiers.wherePredicates.size() == 1 && nonVarQualifiers.tableRowGenerators.size() == 1) {
            final BinaryOperatorPredicate op = nonVarQualifiers.wherePredicates.get(0);
            final TableRowGenerator tr = nonVarQualifiers.tableRowGenerators.get(0);
            final Optional<DetermineIndexes.IndexDescription> idx =
                    DetermineIndexes.maybeIndex(op, tr.getTable());
            if (idx.isPresent()) {
                return List.of(indexedAccess(idx.get(), op));
            }
        }
        return forLoopsOrIndicesFromTableRowGenerators(nonVarQualifiers.tableRowGenerators,
                                                       nonVarQualifiers.joinPredicates);
    }

    private List<CodeBlock> forLoopsOrIndicesFromTableRowGenerators(final List<TableRowGenerator> tableRowGenerators,
                                                                    final List<JoinPredicate> joinPredicates) {
        final TableRowGenerator forLoopTable = tableRowGenerators.get(0);
        final List<CodeBlock> loopStatements = new ArrayList<>();
        loopStatements.add(forLoopFromTableRowGeneratorBlock(forLoopTable));
        // XXX: Use the IndexDescription from the DetermineIndexes pass instead
        tableRowGenerators.subList(1, tableRowGenerators.size()).stream()
            .map(tr -> {
                if (configUseIndicesForEqualityBasedJoins) {
                    for (final BinaryOperatorPredicate binaryOp: joinPredicates) {
                        final Optional<DetermineIndexes.IndexDescription> idx =
                                DetermineIndexes.maybeIndex(binaryOp, tr.getTable());
                        if (idx.isPresent()) {
                            return indexedAccess(idx.get(), binaryOp);
                        }
                    }
                }
                LOG.warn("{} are being iterated using nested for loops", tableRowGenerators);
                return forLoopFromTableRowGeneratorBlock(tr);
            })
            .forEach(loopStatements::add);
        return loopStatements;
    }

    private CodeBlock indexedAccess(final DetermineIndexes.IndexDescription idx,
                                    final BinaryOperatorPredicate op) {
        final DetermineIndexes.IndexedAccess access = idx.toIndexedAccess(op);
        final IRTable indexedTable = access.indexedColumn.getField().getIRTable();
        final IRTable scanTable = access.scanColumn.getField().getIRTable();
        final String idxIterStr = iterStr(indexedTable.getAliasedName());
        final String idxName = indexName(idx);
        final String fieldAccessFromScan = fieldNameStrWithIter(scanTable.getName(),
                                                                access.scanColumn.getField().getName(),
                                                                iterStr(scanTable.getAliasedName()));
        return CodeBlock.builder()
                .addStatement("final List<Integer> $1LList = $2L.get($3L)", idxIterStr, idxName,
                                                                            fieldAccessFromScan)
                .add("for (int $1L : $1LList)", idxIterStr)
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
                .map(expr -> toJavaExpression(expr, context))
                .map(JavaExpression::asString)
                .collect(Collectors.joining(" \n    && "));
        final String wherePredicateStr = nonVarQualifiers.wherePredicates.stream()
                                                .map(expr -> toJavaExpression(expr, context))
                                                .map(JavaExpression::asString)
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
                                                                   final TranslationContext translationContext) {
        final String predicateStr = nonVarQualifiers.aggregatePredicates.stream()
                .map(expr -> toJavaExpression(expr, translationContext))
                .map(JavaExpression::asString)
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
    private OutputIR.Block resultSetAddBlock(final String viewName, final List<Expr> headItems,
                                             @Nullable final GroupByQualifier groupByQualifier,
                                             final TranslationContext context) {
        final OutputIR.Block block = outputIR.newBlock(viewName + "AddToResultSet");
        final String headItemsStr = headItems.stream()
                .map(expr -> {
                    final String fieldNameComment = expr instanceof ColumnIdentifier ?
                             " /* " + ((ColumnIdentifier) expr).getField().getName() + " */" : "";
                    return toJavaExpression(expr, context).asString() + fieldNameComment;
                })
                .collect(Collectors.joining(",\n    "));
        // Create a tuple for the result set
        block.addBody(statement("final var $L = new Tuple$L<>(\n    $L\n    )",
                                context.getTupleVarName(), headItems.size(), headItemsStr));
        final String resultSetNameStr = nonConstraintViewName(viewName);

        // Update result set
        if (groupByQualifier != null) {
            final int numberOfGroupByColumns = groupByQualifier.getGroupByExprs().size();
            // Comma separated list of field accesses to construct a group string
            final String groupString = groupByQualifier.getGroupByExprs().stream()
                    .map(e -> toJavaExpression(e, context))
                    .map(JavaExpression::asString)
                    .collect(Collectors.joining(",     \n"));

            // Organize the collected tuples from the nested for loops by groupByTuple
            block.addBody(statement("final var groupByTuple = new Tuple$L<>(\n    $L\n    )",
                                    numberOfGroupByColumns, groupString));
            block.addBody(statement("$L.computeIfAbsent(groupByTuple, (k) -> new $T<>()).add($L)",
                                     resultSetNameStr, ArrayList.class, context.getTupleVarName()));
        } else {
            block.addBody(statement("$L.add($L)", resultSetNameStr, context.getTupleVarName()));
        }
        return block;
    }

    private OutputIR.Block resultSetAddBlock(final String viewName, final ListComprehension comprehension,
                                             @Nullable final GroupByQualifier groupByQualifier,
                                             final TranslationContext context) {
        final OutputIR.Block block = outputIR.newBlock(viewName + "AddToResultSet");
        if (groupByQualifier != null) {
            final int numberOfGroupByColumns = groupByQualifier.getGroupByExprs().size();
            // Comma separated list of field accesses to construct a group string
            final String groupString = groupByQualifier.getGroupByExprs().stream()
                    .map(e -> toJavaExpression(e, context))
                    .map(JavaExpression::asString)
                    .collect(Collectors.joining(",     \n"));

            // Organize the collected tuples from the nested for loops by groupByTuple
            block.addBody(statement("final var groupByTuple = new Tuple$L<>(\n    $L\n    )",
                    numberOfGroupByColumns, groupString));
        }
        comprehension.getQualifiers().stream()
                .filter(expr -> expr instanceof TableRowGenerator)
                .forEach(expr -> {
                    final IRTable table = ((TableRowGenerator) expr).getTable();
                    final String iterStr = iterStr(table.getName());
                    final String tableName = tableNameStr(table.getName());
                    final String resultSetNameStr =  nonConstraintViewName(viewName);
                    if (groupByQualifier != null) {
                        block.addBody(statement("$L.computeIfAbsent(groupByTuple, (k) -> new $T<>())" +
                                                ".computeIfAbsent($S, (k) -> new $T<>()).add($L)",
                                                resultSetNameStr, HashMap.class, tableName, HashSet.class, iterStr));
                    } else {
                        block.addBody(statement("$L.computeIfAbsent($S, (k) -> new $T()).add($L)",
                                resultSetNameStr, tableName, HashSet.class, iterStr));
                    }
                });
        return block;
    }

    /**
     * Returns a list of code blocks representing constraints posted against rows within a view
     */
    private List<CodeBlock> rowConstraintBlock(final QualifiersByVarType qualifiersByVarType,
                                               final ConstraintType constraintType,
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
            joinPredicateStr = toJavaExpression(combinedJoinPredicate, context)
                                    .asString();
        } else {
            joinPredicateStr = "";
        }
        final List<CodeBlock> results = new ArrayList<>();
        varQualifiers.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), joinPredicateStr, constraintType, context)));
        nonVarQualifiers.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), joinPredicateStr, constraintType, context)));
        return results;
    }

    /**
     * Returns a list of code blocks representing aggregate constraints posted against rows within a view
     */
    private List<CodeBlock> aggregateConstraintBlock(final QualifiersByVarType qualifiersByVarType,
                                                     final ConstraintType constraintType,
                                                     final TranslationContext context) {
        final List<CodeBlock> results = new ArrayList<>();
        qualifiersByVarType.var.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), "", constraintType, context)));
        qualifiersByVarType.nonVar.checkQualifiers.forEach(e ->
                results.add(topLevelConstraintBlock(e.getExpr(), "", constraintType, context)));
        return results;
    }

    /**
     * Creates a string representing a declared constraint
     */
    private CodeBlock topLevelConstraintBlock(final Expr expr, final String joinPredicateStr,
                                              final ConstraintType constraintType,
                                              final TranslationContext context) {
        final String statement = maybeWrapped(expr, context);
        switch (constraintType) {
            case HARD_CONSTRAINT:
                if (joinPredicateStr.isEmpty()) {
                    return CodeBlock.builder().addStatement("o.assume($L, $S)",
                            statement, context.currentScope().getName())
                            .build();
                } else {
                    return CodeBlock.builder().addStatement("o.assumeImplication($L, $L, $S)",
                            joinPredicateStr, statement, context.currentScope().getName())
                            .build();
                }
            case OBJECTIVE:
                if (joinPredicateStr.isEmpty()) {
                    return CodeBlock.builder().addStatement("o.maximize($L) /* $S */",
                            statement, context.currentScope().getName()).build();
                } else {
                    throw new IllegalArgumentException("Does not support join predicate strings yet");
                }
            default:
        }
        throw new IllegalArgumentException("Should not be here for non constraint queries");
    }

    /**
     * Wrap constants 'x' in model.newConstant(x) depending on the type. Also converts true/false to 1/0.
     */
    private String maybeWrapped(final Expr expr, final TranslationContext context) {
        final JavaExpression javaExpr = toJavaExpression(expr, context);
        String exprStr = javaExpr.asString();

        // Some special cases to handle booleans because the or-tools API does not convert well to booleans
        if (expr instanceof Literal && javaExpr.type() == JavaType.Boolean) {
            exprStr = (Boolean) ((Literal<?>) expr).getValue() ? "1" : "0";
        }
        return javaExpr.type() == JavaType.IntVar ? exprStr : String.format("o.toConst(%s)", exprStr);
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
            final JavaTypeList recordTypeParameters = tupleMetadata.recordTableTupleType(table);

            if (table.isAliasedTable()) {
                continue;
            }
            output.addCode("\n");
            output.addCode("/* Table $S */\n", table.getName());
            // ...2) create a List<[RecordType]> to represent the table
            output.addStatement("final $T<$T<$L>> $L = (List<$T<$L>>) $L.get($S)",
                                 List.class, recordType, recordTypeParameters, tableNameStr(table.getName()),
                                 recordType, recordTypeParameters, INPUT_DATA_VARIABLE, table.getName());

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
                            .addStatement("$L[i] = o.newIntVar($S + \"[\" + i + \"]\")", variableName, fieldName)
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
                                "final long[] domain$L = $L.get($S)",
                                "                        .getValues($S, $L.class)",
                                "                        .stream()",
                                "                        .mapToLong(encoder::toLong).toArray()"
                        );
                        final JavaType parentType = tupleMetadata.inferType(parent);
                        output.addStatement(snippet,
                               fieldIndex.get(), INPUT_DATA_VARIABLE, parent.getIRTable().getName(),
                               parent.getName().toUpperCase(Locale.US), parentType.typeString());
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
        final ClassName stringClass = ClassName.get(String.class);
        final ClassName map = ClassName.get(Map.class);
        // Map<IRTable, Result<? extends Record>>
        final ParameterizedTypeName returnT = ParameterizedTypeName.get(map, stringClass, resultT);
        output.addModifiers(Modifier.PUBLIC)
               .returns(returnT)
               .addParameter(returnT, INPUT_DATA_VARIABLE, Modifier.FINAL)
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
               .addStatement("final $T solver = o.solve($L /* numThreads */, $L /* maxTimeInSeconds*/)",
                             CpSolver.class, configNumThreads, configMaxTimeInSeconds)
               .addStatement("final Map<String, Result<? extends Record>> result = new $T<>()", HashMap.class)
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
                        "$L.get($S)", tempViewName, INPUT_DATA_VARIABLE, tableName);

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
                output.addStatement("result.put($S, $L)", tableName, tempViewName);
            }
        }
        output.addStatement("return result");
    }

    static String camelCase(final String name) {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name.toUpperCase());
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
            if (tupleMetadata.isView(tableName)) {
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

    private static String indexName(final DetermineIndexes.IndexDescription index) {
        final IRTable table = index.relation;
        final IRColumn column = index.columnBeingAccessed.getField();
        return tableNameStr(table.getName()) + fieldNameStr(table.getName(), column.getName()) + "Index";
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
    public boolean needsGroupTables() {
        return false;
    }

    private JavaExpression toJavaExpression(final Expr expr, final TranslationContext context) {
        final IRToJavaExpression visitor = new IRToJavaExpression();
        return visitor.visit(expr, context);
    }

    private QualifiersByVarType extractQualifiersByVarType(final ListComprehension comprehension) {
        final QualifiersByType varQualifiers = new QualifiersByType();
        final QualifiersByType nonVarQualifiers = new QualifiersByType();
        comprehension.getQualifiers().forEach(
            q -> {
                if (UsesControllableFields.apply(q)) {
                    varQualifiers.addQualifierByType(q);
                } else {
                    nonVarQualifiers.addQualifierByType(q);
                }
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
     * The main logic to parse a comprehension and translate it into a set of intermediate variables and expressions.
     */
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE") // false positive
    private class IRToJavaExpression extends IRVisitor<JavaExpression, TranslationContext> {

        @Override
        protected JavaExpression visitFunctionCall(final FunctionCall node, final TranslationContext context) {
            final String vectorName = context.isSubQueryContext() ? context.getSubQueryContext().getSubQueryName()
                                                                  : context.getGroupContext().getGroupViewName();
            // Functions always apply on a vector. We compute the arguments to the function, and in doing so,
            // add declarations to the corresponding for-loop that extracts the relevant columns/expressions from views.
            final OutputIR.Block forLoop = context.currentScope().getForLoopByName(vectorName);
            switch (node.getFunction()) {
                // Start with unary functions
                case SUM:
                case COUNT:
                case MAX:
                case MIN:
                case ANY:
                case ALL:
                case ALL_EQUAL:
                case ALL_DIFFERENT:
                case INCREASING:
                    context.enterScope(forLoop);
                    final JavaExpression processedArgument = visit(node.getArgument().get(0),
                            context.withEnterFunctionContext());
                    context.leaveScope();
                    final JavaExpression listOfProcessedItem = extractListFromLoop(processedArgument,
                            context.currentScope(), forLoop);
                    final boolean supportsAssumptions = supportsAssumptions(node);
                    final JavaType argType = processedArgument.type();
                    final JavaType outType = tupleMetadata.inferType(node);
                    final String functionName = String.format("%s%s", camelCase(node.getFunction().toString()),
                                                              argType);
                    final String argumentString = listOfProcessedItem.asString();
                    return supportsAssumptions ?
                            // Use the current scope as the assumption context
                           new JavaExpression(CodeBlock.of("o.$L($L, $S)", functionName, argumentString,
                                                           context.currentScope().getName()).toString(), outType)
                           : new JavaExpression(CodeBlock.of("o.$L($L)", functionName, argumentString).toString(),
                                                outType);
                // Binary functions
                case SCALAR_PRODUCT:
                    context.enterScope(forLoop);
                    final JavaExpression arg1 = visit(node.getArgument().get(0),
                                                                   context.withEnterFunctionContext());
                    final JavaExpression arg2 = visit(node.getArgument().get(1),
                                                                   context.withEnterFunctionContext());
                    context.leaveScope();
                    final JavaExpression listOfArg1 = extractListFromLoop(arg1, context.currentScope(), forLoop);
                    final JavaExpression listOfArg2 = extractListFromLoop(arg2, context.currentScope(), forLoop);
                    final JavaType arg2Type = arg2.type();
                    return new JavaExpression(CodeBlock.of("o.scalProd$L($L, $L)", arg2Type.toString(),
                                              listOfArg1.asString(), listOfArg2.asString()).toString(),
                                              JavaType.IntVar);
                case CAPACITY_CONSTRAINT:
                    final Map<String, OutputIR.ForBlock> tableToForBlock = new HashMap<>();
                    for (int i = 0; i < 4; i++) {
                        final String tableName = ((ColumnIdentifier) node.getArgument().get(i)).getTableName();
                        if (tableToForBlock.containsKey(tableName)) {
                            continue;
                        }
                        final CodeBlock codeBlock = CodeBlock.of("for (int $L : $L.get($S))",
                                iterStr(tableName), context.getGroupContext().getGroupDataName(),
                                tableNameStr(tableName));
                        final OutputIR.ForBlock forBlock =
                                outputIR.newForBlock(tableName + "CapacityConstraintLoop", codeBlock);
                        context.currentScope().addBody(forBlock);
                        tableToForBlock.put(tableName, forBlock);
                    }
                    final Set<String> vars = new HashSet<>();
                    final Set<String> domain = new HashSet<>();
                    final List<String> demands = new ArrayList<>();
                    final List<String> capacities = new ArrayList<>();
                    for (int i = 0; i < 4; i++) {
                        final Expr arg = node.getArgument().get(i);
                        Preconditions.checkArgument(arg instanceof ColumnIdentifier);
                        final ColumnIdentifier columnArg = (ColumnIdentifier) arg;
                        final OutputIR.ForBlock forBlock = tableToForBlock.get(columnArg.getTableName());
                        context.enterScope(forBlock);
                        final JavaExpression variable = toJavaExpression(columnArg, context);
                        final boolean shouldCoerce = (i == 2 || i == 3) && variable.type() != JavaType.Long;
                        final JavaExpression maybeCoercedVariable;
                        if (shouldCoerce) {
                            // if demands/capacities are in Integer domain, we coerce to longs
                            forBlock.addBody(statement("final long $L_l = (long) $L", variable.asString(),
                                    variable.asString()));
                            maybeCoercedVariable = new JavaExpression(variable.asString() + "_l", JavaType.Long);
                        } else {
                            maybeCoercedVariable = variable;
                        }
                        context.leaveScope();
                        final JavaExpression parameter = extractListFromLoop(maybeCoercedVariable,
                                                                             context.currentScope(),
                                                                             forBlock);
                        if (i == 0) { // vars
                            vars.add(parameter.asString());
                        } else if (i == 1) { // domain
                            domain.add(parameter.asString());
                        } else if (i == 2) { // demands
                            demands.add(parameter.asString());
                        } else { // capacities
                            capacities.add(parameter.asString());
                        }
                    }
                    final String varsParameterStr = vars.iterator().next();
                    final String domainParameterStr = domain.iterator().next();
                    final String demandsParameterStr = String.join(", ", demands);
                    final String capacitiesParameterStr = String.join(", ", capacities);
                    final CodeBlock constraintExpr = CodeBlock.of(
                            "o.capacityConstraint($L, $L, $T.of($L), $T.of($L), $L)",
                            varsParameterStr, domainParameterStr,
                            List.class, demandsParameterStr, List.class, capacitiesParameterStr,
                            configUseCapacityPresenceLiterals);
                    return new JavaExpression(constraintExpr.toString(), JavaType.IntVar);
                default:
                    throw new UnsupportedOperationException("Unsupported aggregate function " + node.getFunction());
            }
        }

        private boolean supportsAssumptions(final FunctionCall node) {
            switch (node.getFunction()) {
                case ALL_DIFFERENT:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        protected JavaExpression visitExistsPredicate(final ExistsPredicate node, final TranslationContext context) {
            final JavaExpression processedArgument = visit(node.getArgument(), context);
            return context.declare(String.format("o.exists(%s)", processedArgument.asString()), JavaType.IntVar);
        }

        @Override
        protected JavaExpression visitIsNullPredicate(final IsNullPredicate node, final TranslationContext context) {
            final JavaExpression processedArgument = visit(node.getArgument(), context);
            Preconditions.checkArgument(processedArgument.type() != JavaType.IntVar);
            return context.declare(String.format("%s == null", processedArgument.asString()), JavaType.Boolean);
        }

        @Override
        protected JavaExpression visitIsNotNullPredicate(final IsNotNullPredicate node,
                                                         final TranslationContext context) {
            final JavaExpression processedArgument = visit(node.getArgument(), context);
            Preconditions.checkArgument(processedArgument.type() != JavaType.IntVar);
            return context.declare(String.format("%s != null", processedArgument.asString()),  JavaType.Boolean);
        }

        @Override
        protected JavaExpression visitUnaryOperator(final UnaryOperator node,
                                                    final TranslationContext context) {
            final JavaExpression id = visit(node.getArgument(), context);
            switch (node.getOperator()) {
                case NOT:
                    Preconditions.checkArgument(id.type() == JavaType.IntVar ||
                                                id.type() == JavaType.Boolean);
                    return context.declare(String.format("o.not(%s)", id.asString()), id.type());
                case MINUS:
                    Preconditions.checkArgument(id.type() == JavaType.IntVar
                                                || id.type() == JavaType.Integer
                                                || id.type() == JavaType.Long);
                    return context.declare(String.format("o.mult(-1, %s)", id.asString()), id.type());
                case PLUS:
                    Preconditions.checkArgument(id.type() == JavaType.IntVar
                                                || id.type() == JavaType.Integer
                                                || id.type() == JavaType.Long);
                    return context.declare(id);
                default:
                    throw new IllegalArgumentException(node.toString());
            }
        }

        @Override
        protected JavaExpression visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                              final TranslationContext context) {
            final JavaExpression leftArg = visit(node.getLeft(), context);
            final JavaExpression rightArg = visit(node.getRight(), context);
            final String left = leftArg.asString();
            final String right = rightArg.asString();
            final BinaryOperatorPredicate.Operator op = node.getOperator();

            if (leftArg.type() == JavaType.IntVar || rightArg.type() == JavaType.IntVar) {
                // We need to generate an IntVar.
                switch (op) {
                    case EQUAL:
                        return context.declare(String.format("o.eq(%s, %s)", left, right), JavaType.IntVar);
                    case NOT_EQUAL:
                        return context.declare(String.format("o.ne(%s, %s)", left, right), JavaType.IntVar);
                    case AND:
                        return context.declare(String.format("o.and(%s, %s)", left, right), JavaType.IntVar);
                    case OR:
                        return context.declare(String.format("o.or(%s, %s)", left, right), JavaType.IntVar);
                    case LESS_THAN_OR_EQUAL:
                        return context.declare(String.format("o.leq(%s, %s)", left, right), JavaType.IntVar);
                    case LESS_THAN:
                        return context.declare(String.format("o.lt(%s, %s)", left, right), JavaType.IntVar);
                    case GREATER_THAN_OR_EQUAL:
                        return context.declare(String.format("o.geq(%s, %s)", left, right), JavaType.IntVar);
                    case GREATER_THAN:
                        return context.declare(String.format("o.gt(%s, %s)", left, right), JavaType.IntVar);
                    case ADD:
                        return context.declare(String.format("o.plus(%s, %s)", left, right), JavaType.IntVar);
                    case SUBTRACT:
                        return context.declare(String.format("o.minus(%s, %s)", left, right), JavaType.IntVar);
                    case MULTIPLY:
                        return context.declare(String.format("o.mult(%s, %s)", left, right), JavaType.IntVar);
                    case DIVIDE:
                        return context.declare(String.format("o.div(%s, %s)", left, right), JavaType.IntVar);
                    case NOT_IN:
                        return context.declare(String.format("o.notIn%s(%s, %s)",
                                rightArg.type().innerType().orElseThrow(), left, right), JavaType.IntVar);
                    case IN:
                        return context.declare(String.format("o.in%s(%s, %s)",
                                rightArg.type().innerType().orElseThrow(), left, right), JavaType.IntVar);
                    case CONTAINS:
                        return context.declare(String.format("o.inObjectArr(%s, %s)", right, left), JavaType.IntVar);
                    default:
                        throw new UnsupportedOperationException("Operator " + op);
                }
            }
            else {
                // Both operands are non-var, so we generate an expression in Java.
                switch (op) {
                    case EQUAL:
                        return context.declare(String.format("(o.eq(%s, %s))", left, right), JavaType.Boolean);
                    case NOT_EQUAL:
                        return context.declare(String.format("(!o.eq(%s, %s))", left, right), JavaType.Boolean);
                    case AND:
                        return context.declare(String.format("(%s && %s)", left, right), JavaType.Boolean);
                    case OR:
                        return context.declare(String.format("(%s || %s)", left, right), JavaType.Boolean);
                    case CONTAINS:
                        return context.declare(String.format("o.in(%s, %s)", right, left), JavaType.Boolean);
                    case NOT_IN:
                        return context.declare(String.format("!o.in(%s, %s)", left, right), JavaType.Boolean);
                    case IN:
                        return context.declare(String.format("(o.in(%s, %s))", left, right), JavaType.Boolean);
                    case LESS_THAN_OR_EQUAL:
                        return context.declare(String.format("(%s <= %s)", left, right), JavaType.Boolean);
                    case LESS_THAN:
                        return context.declare(String.format("(%s < %s)", left, right), JavaType.Boolean);
                    case GREATER_THAN_OR_EQUAL:
                        return context.declare(String.format("(%s >= %s)", left, right), JavaType.Boolean);
                    case GREATER_THAN:
                        return context.declare(String.format("(%s > %s)", left, right), JavaType.Boolean);
                    case ADD:
                        return context.declare(String.format("(%s + %s)", left, right), JavaType.Long);
                    case SUBTRACT:
                        return context.declare(String.format("(%s - %s)", left, right), JavaType.Long);
                    case MULTIPLY:
                        return context.declare(String.format("(%s * %s)", left, right), JavaType.Long);
                    case DIVIDE:
                        return context.declare(String.format("(%s / %s)", left, right), JavaType.Long);
                    default:
                        throw new UnsupportedOperationException("Operator " + op);
                }
            }
        }

        @Override
        protected JavaExpression visitColumnIdentifier(final ColumnIdentifier node, final TranslationContext context) {
            final JavaType type = tupleMetadata.inferType(node);

            // Sub-queries also use an intermediate view, and we again need an indirection from column names to indices
            if (context.isSubQueryContext()) {
                final String tempTableName = context.getSubQueryContext().getSubQueryName().toUpperCase(Locale.US);
                final int fieldIndex = tupleMetadata.getFieldIndexInView(tempTableName, node.getField().getName());
                return context.declare(String.format("%s.value%s()", context.getTupleVarName(), fieldIndex), type);
            }

            // Check whether this is referring to a group by column.
            // If we are evaluating a group-by comprehension, then column accesses that happen outside the context
            // of an aggregation function must refer to the grouping column, not the inner tuple being iterated over.
            if (!context.isFunctionContext() && context.isGroupContext()) {
                final String tableName = node.getTableName();
                final String fieldName = node.getField().getName();
                int columnNumber = 0;
                final GroupContext groupContext = context.getGroupContext();
                final String groupName = groupContext.getGroupName();
                for (final ColumnIdentifier ci: groupContext.getQualifier().getGroupByColumnIdentifiers()) {
                    if (ci.getTableName().equalsIgnoreCase(tableName)
                            && ci.getField().getName().equalsIgnoreCase(fieldName)) {
                        return context.declare(String.format("%s.value%s()", groupName, columnNumber), type);
                    }
                    columnNumber++;
                }
                // This is not a grouping column access. Fall through
            }

            // Within a group-by, we refer to values from the intermediate group by table. This involves an
            // indirection from columns to tuple indices
            if (context.isFunctionContext() && context.isGroupContext()) {
                final String tempTableName = context.getGroupContext().getTempTableName().toUpperCase(Locale.US);
                final int fieldIndex = tupleMetadata.getFieldIndexInView(tempTableName, node.getField().getName());
                final String groupDataTupleName = context.getGroupContext().getGroupDataTupleName();
                return context.declare(String.format("%s.value%s()", groupDataTupleName, fieldIndex), type);
            }

            // Simple field access
            final String tableName = node.getField().getIRTable().getName();
            final String fieldName = node.getField().getName();
            final String iterStr = iterStr(node.getTableName());
            return context.declare(fieldNameStrWithIter(tableName, fieldName, iterStr), type);
        }

        @Override
        protected JavaExpression visitLiteral(final Literal node, final TranslationContext context) {
            final JavaType type = tupleMetadata.inferType(node);
            if (node.getValue() instanceof String) {
                return new JavaExpression(node.getValue().toString().replace("'", "\""), type);
            } else if (node.getValue() instanceof Long) {
                return new JavaExpression(node.getValue() + "L", type);
            } else {
                return new JavaExpression(node.getValue().toString(), type);
            }
        }

        @Override
        protected JavaExpression visitListComprehension(final ListComprehension node,
                                                        final TranslationContext context) {
            // We are in a subquery.
            final String newSubqueryName = context.getNewSubqueryName();
            final OutputIR.Block subQueryBlock = viewBlock(newSubqueryName, node, ConstraintType.NON_CONSTRAINT,
                                                           context);
            Preconditions.checkArgument(node.getHead().getSelectExprs().size() == 1);
            final Expr headSelectItem = node.getHead().getSelectExprs().get(0);

            final ContainsFunctionCall visitor = new ContainsFunctionCall();
            visitor.visit(headSelectItem);
            final boolean headContainsFunctionCall = visitor.getFound();

            attemptConstantSubqueryOptimization(node, subQueryBlock, context);

            final TranslationContext newCtx = context.withEnterSubQueryContext(newSubqueryName);
            // If the head contains a function, then this is a scalar subquery
            if (headContainsFunctionCall) {
                newCtx.enterScope(subQueryBlock);
                final JavaExpression processedItem = toJavaExpression(headSelectItem, newCtx);
                final JavaExpression ret = context.declare(processedItem);
                newCtx.leaveScope();
                return ret;
            } else {
                // Else, treat the result as a vector
                newCtx.enterScope(subQueryBlock.getForLoopByName(newSubqueryName));
                final JavaExpression processedHeadItem = toJavaExpression(node.getHead().getSelectExprs().get(0),
                                                                          newCtx);
                final JavaExpression list = extractListFromLoop(processedHeadItem, subQueryBlock, newSubqueryName);
                newCtx.leaveScope();
                return declare(list, subQueryBlock, context);
            }
        }

        @Override
        protected JavaExpression visitGroupByComprehension(final GroupByComprehension node,
                                                           final TranslationContext context) {
            // We are in a subquery.
            final String newSubqueryName = context.getNewSubqueryName();
            final OutputIR.Block subQueryBlock = viewBlock(newSubqueryName, node, ConstraintType.NON_CONSTRAINT,
                                                           context);
            Preconditions.checkArgument(node.getComprehension().getHead().getSelectExprs().size() == 1);
            final Expr headSelectItem = node.getComprehension().getHead().getSelectExprs().get(0);

            final TranslationContext newCtx = context.withEnterSubQueryContext(newSubqueryName);
            attemptConstantSubqueryOptimization(node, subQueryBlock, context);

            // if scalar subquery
            if (headSelectItem instanceof FunctionCall) {
                newCtx.enterScope(subQueryBlock);
                final JavaExpression processedItem = toJavaExpression(headSelectItem, newCtx);
                final JavaExpression ret = context.declare(processedItem);
                newCtx.leaveScope();
                return ret;
            } else {
                newCtx.enterScope(subQueryBlock.getForLoopByName(newSubqueryName));
                final JavaExpression processedHeadItem =
                    toJavaExpression(node.getComprehension().getHead().getSelectExprs().get(0), newCtx);
                final JavaExpression list =
                        extractListFromLoop(processedHeadItem, subQueryBlock, newSubqueryName);
                newCtx.leaveScope();
                // Treat as a vector
                return declare(list, subQueryBlock, context);
            }
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
        protected JavaExpression declare(final JavaExpression expression, final OutputIR.Block block,
                                         final TranslationContext context) {
            return new JavaExpression(context.declareVariable(expression.asString(), block), expression.type());
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
     * @return the name of the list being extracted
     */
    private JavaExpression extractListFromLoop(final JavaExpression variableToExtract, final OutputIR.Block outerBlock,
                                               final OutputIR.Block innerBlock) {
        final String listName = "listOf" + variableToExtract.asString();
        // For computing aggregates, the loop blocks have a valid size. Use this for pre-allocating lists.
        final String maybeGuessSize = innerBlock instanceof OutputIR.ForBlock ?
                ((OutputIR.ForBlock) innerBlock).getSize() : "";
        final boolean wasAdded = outerBlock.addHeader(statement("final List<$L> listOf$L = new $T<>($L)",
                variableToExtract.type().typeString(), variableToExtract.asString(), ArrayList.class, maybeGuessSize));
        if (wasAdded) {
            innerBlock.addBody(statement("$L.add($L)", listName, variableToExtract.asString()));
        }
        return new JavaExpression(listName, JavaType.listType(variableToExtract.type()));
    }

    /**
     * Used to extract a variable computed within each iteration of a loop into a list for later use (for
     * example, aggregate functions).
     *
     * @param variableToExtract a variable name from within a block to extract out of a loop
     * @param outerBlock the block outside the loop where the extracted list will be created
     * @param loopBlockName the name of the loop from which we want to extract a list
     * @return the name of the list being extracted
     */
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // false positive
    private JavaExpression extractListFromLoop(final JavaExpression variableToExtract, final OutputIR.Block outerBlock,
                                               final String loopBlockName) {
        final OutputIR.Block forLoop = outerBlock.getForLoopByName(loopBlockName);
        return extractListFromLoop(variableToExtract, outerBlock, forLoop);
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
                LOG.trace("Ignoring group-by qualifier");
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

    private enum ConstraintType {
        NON_CONSTRAINT,
        HARD_CONSTRAINT,
        OBJECTIVE
    }
}