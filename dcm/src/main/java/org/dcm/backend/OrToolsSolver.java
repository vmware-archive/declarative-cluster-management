/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.esotericsoftware.reflectasm.ConstructorAccess;
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
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.ModelException;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.ExistsPredicate;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.GroupByQualifier;
import org.dcm.compiler.monoid.IsNotNullPredicate;
import org.dcm.compiler.monoid.IsNullPredicate;
import org.dcm.compiler.monoid.JoinPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.MonoidVisitor;
import org.dcm.compiler.monoid.Qualifier;
import org.dcm.compiler.monoid.TableRowGenerator;
import org.dcm.compiler.monoid.UnaryOperator;
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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
    public static final String OR_TOOLS_LIB_ENV = "OR_TOOLS_LIB";
    private static final String GENERATED_BACKEND_CLASS_FILE_PATH = "/tmp";
    private static final Logger LOG = LoggerFactory.getLogger(OrToolsSolver.class);
    private static final String GENERATED_BACKEND_NAME = "GeneratedBackend";
    private static final String GENERATED_FIELD_NAME_PREFIX = "GenField";
    private static final String SUBQUERY_NAME_PREFIX = "subquery";
    private static final MethodSpec INT_VAR_NO_BOUNDS = MethodSpec.methodBuilder("INT_VAR_NO_BOUNDS")
                                    .addModifiers(Modifier.PRIVATE)
                                    .addParameter(CpModel.class, "model", Modifier.FINAL)
                                    .addParameter(String.class, "name",  Modifier.FINAL)
                                    .returns(IntVar.class)
                                    .addStatement("return model.newIntVar(Integer.MIN_VALUE, Integer.MAX_VALUE, name)")
                                    .build();
    private final Map<String, Map<String, Integer>> viewToFieldIndex = new HashMap<>();
    private final AtomicInteger generatedFieldNameCounter = new AtomicInteger(0);
    private final AtomicInteger intermediateViewCounter = new AtomicInteger(0);
    private final AtomicInteger subqueryCounter = new AtomicInteger(0);
    private final Map<String, Map<String, String>> tableToFieldToType = new HashMap<>();
    private final Map<String, String> viewTupleTypeParameters = new HashMap<>();
    private final Map<String, String> viewGroupByTupleTypeParameters = new HashMap<>();
    private final TupleGen tupleGen = new TupleGen();

    static {
        Preconditions.checkNotNull(System.getenv(OR_TOOLS_LIB_ENV));
        System.load(System.getenv(OR_TOOLS_LIB_ENV));
    }

    @Nullable private IGeneratedBackend generatedBackend;
    @Nullable private IRContext context = null;

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
                                          final Map<String, MonoidComprehension> nonConstraintViews,
                                          final Map<String, MonoidComprehension> constraintViews,
                                          final Map<String, MonoidComprehension> objectiveFunctions) {
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
        nonConstraintViews
                .forEach((name, comprehension) -> {
                    final MonoidComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    final ExprContext exprContext = new ExprContext(false);
                    final OutputIR.Block outerBlock = new OutputIR.Block("outer");
                    exprContext.enterScope(outerBlock);
                    final OutputIR.Block block = addView(name, rewrittenComprehension, false, exprContext);
                    exprContext.leaveScope();
                    output.addCode(block.toString());
                });
        constraintViews
                .forEach((name, comprehension) -> {
                    final MonoidComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    final ExprContext exprContext = new ExprContext(false);
                    final OutputIR.Block outerBlock = new OutputIR.Block("outer");
                    exprContext.enterScope(outerBlock);
                    final OutputIR.Block block = addView(name, rewrittenComprehension, true, exprContext);
                    exprContext.leaveScope();
                    output.addCode(block.toString());
                });
        objectiveFunctions
                .forEach((name, comprehension) -> {
                    final MonoidComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    final ExprContext objFunctionContext = new ExprContext(false);
                    final OutputIR.Block outerBlock = new OutputIR.Block("outer");
                    objFunctionContext.enterScope(outerBlock);
                    final String exprStr = exprToStr(rewrittenComprehension, objFunctionContext);
                    objFunctionContext.leaveScope();
                    output.addCode(outerBlock.toString());
                    output.addStatement("final $T $L = $L", IntVar.class, name, exprStr);
                });
        if (!objectiveFunctions.isEmpty()) {
            final String objectiveFunctionSum = String.join(", ", objectiveFunctions.keySet());
            output.addStatement("model.maximize(o.sumV($T.of($L)))", List.class, objectiveFunctionSum);
        }

        addSolvePhase(output, context);
        final MethodSpec solveMethod = output.build();

        final TypeSpec.Builder backendClassBuilder = TypeSpec.classBuilder(GENERATED_BACKEND_NAME)
                .addAnnotation(AnnotationSpec.builder(Generated.class)
                                 .addMember("value", "$S", "org.dcm.backend.OrToolsSolver")
                                 .build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(IGeneratedBackend.class)
                .addMethod(solveMethod)
                .addMethod(INT_VAR_NO_BOUNDS);
        tupleGen.getAllTupleTypes().forEach(backendClassBuilder::addType); // Add tuple types

        final TypeSpec spec = backendClassBuilder.build();
        return compile(spec);
    }

    private OutputIR.Block addView(final String viewName, final MonoidComprehension comprehension,
                                   final boolean isConstraint, final ExprContext context) {
        if (comprehension instanceof GroupByComprehension) {
            final GroupByComprehension groupByComprehension = (GroupByComprehension) comprehension;
            final MonoidComprehension inner = groupByComprehension.getComprehension();
            final GroupByQualifier groupByQualifier = groupByComprehension.getGroupByQualifier();
            final OutputIR.Block block = new OutputIR.Block(viewName);

            // Rewrite inner comprehension using the following rule:
            // [ [blah - sum(X) | <....>] | G]
            //                     rows
            //

            // We create an intermediate view that extracts groups and returns a List<Tuple> per group
            final String intermediateView = getTempViewName();
            final OutputIR.Block intermediateViewBlock =
                    buildInnerComprehension(intermediateView, inner, groupByQualifier, isConstraint, context);
            block.addChild(intermediateViewBlock);

            // We now construct the actual result set that hosts the aggregated tuples by group. This is done
            // in two steps...
            final int groupByQualifiersSize = groupByQualifier.getColumnIdentifiers().size();
            final String groupByTupleTypeParameters = viewGroupByTupleTypeParameters.get(intermediateView);
            final String headItemsTupleTypeParamters = viewTupleTypeParameters.get(intermediateView);
            assert inner.getHead() != null;

            // when duplicates appear for viewToFieldIndex, we increment the fieldIndex counter but do not add a new
            // entry. This means that the highest fieldIndex (and not the size of the map) is equal to tuple size.
            // The indices are 0-indexed.
            final int innerTupleSize = Collections.max(viewToFieldIndex.get(intermediateView.toUpperCase(Locale.US))
                                                                       .values()) + 1;
            // (1) Create the result set
            block.addChild(CodeBlock.builder()
                                 .addStatement("\n")
                                 .addStatement(printTime("Group-by intermediate view"))
                                 .addStatement("/* $L view $L */", isConstraint ? "Constraint" : "Non-constraint",
                                                    tableNameStr(viewName))
                                     .build()
            );

            if (!isConstraint) {
                final int selectExprSize = inner.getHead().getSelectExprs().size();
                final TypeSpec typeSpec = tupleGen.getTupleType(selectExprSize);
                final String viewTupleGenericParameters =
                        generateTupleGenericParameters(inner.getHead().getSelectExprs());
                viewTupleTypeParameters.put(tableNameStr(viewName), viewTupleGenericParameters);
                block.addChild(
                    statement("final $T<$N<$L>> $L = new $T<>($L.size())", List.class, typeSpec,
                                                     viewTupleGenericParameters, tableNameStr(viewName),
                                                     ArrayList.class, intermediateView)
                );
            }

            // (2) Loop over the result set collected from the inner comprehension
            final OutputIR.ForBlock forBlock = new OutputIR.ForBlock(viewName,
                    CodeBlock.of("for (final $T<Tuple$L<$L>, List<Tuple$L<$L>>> entry: $L.entrySet())",
                                            Map.Entry.class, groupByQualifiersSize, groupByTupleTypeParameters,
                                            innerTupleSize, headItemsTupleTypeParamters, intermediateView)
            );

            forBlock.addHeader(
                CodeBlock.builder()
                         .addStatement("final Tuple$L<$L> group = entry.getKey()", groupByQualifiersSize,
                                                                                   groupByTupleTypeParameters)
                         .addStatement("final List<Tuple$L<$L>> data = entry.getValue()", innerTupleSize,
                                                                                  headItemsTupleTypeParamters)
                         .build()
            );

            final OutputIR.ForBlock dataForBlock = new OutputIR.ForBlock(viewName,
                        CodeBlock.of("for (final Tuple$L<$L> t: data)", innerTupleSize, headItemsTupleTypeParamters)
            );
            forBlock.addChild(dataForBlock);

            // (3) Filter if necessary
            final QualifiersByType varQualifiers = new QualifiersByType();
            final QualifiersByType nonVarQualifiers = new QualifiersByType();
            populateQualifiersByVarType(inner, varQualifiers, nonVarQualifiers, false);

            final GroupContext groupContext = new GroupContext(groupByQualifier, intermediateView, viewName);
            context.enterScope(forBlock);
            final OutputIR.Block nonVarAggregateFiltersBlock = maybeAddNonVarAggregateFilters(viewName,
                                                                                        nonVarQualifiers, groupContext,
                                                                                        context);
            forBlock.addChild(nonVarAggregateFiltersBlock);

            // If this is not a constraint, we simply add a to a result set
            if (!isConstraint) {
                final TypeSpec typeSpec = tupleGen.getTupleType(inner.getHead().getSelectExprs().size());
                final String viewTupleGenericParameters =
                        generateTupleGenericParameters(inner.getHead().getSelectExprs());
                final String tupleResult = inner.getHead().getSelectExprs().stream()
                                                 .map(e -> exprToStr(e, true, groupContext, context))
                                                 .collect(Collectors.joining(", "));
                forBlock.addChild(
                   CodeBlock.builder().addStatement("final $1N<$2L> t = new $1N<>($3L)", typeSpec,
                                                    viewTupleGenericParameters, tupleResult).build()
                );

                // Record field name indices for view
                // TODO: wrap this block into a helper.
                final AtomicInteger fieldIndex = new AtomicInteger(0);
                inner.getHead().getSelectExprs().forEach(
                        e -> updateFieldIndex(viewName, e, fieldIndex)
                );
                forBlock.addChild(CodeBlock.builder().addStatement("$L.add(t)", tableNameStr(viewName)).build());
            }
            else  {
                // If this is a constraint, we translate having clauses into a constraint statement

                final List<CodeBlock> constraintBlocks = addAggregateConstraint(varQualifiers, nonVarQualifiers,
                        new GroupContext(groupByQualifier, intermediateView, viewName), context);
                constraintBlocks.forEach(forBlock::addChild);
            }
            context.leaveScope();
            block.addChild(forBlock);
            block.addChild(printTime("Group-by final view"));
            return block;
        }
        return buildInnerComprehension(viewName, comprehension, null, isConstraint, context);
    }

    /**
     * Converts a comprehension into a set of nested for loops that return a "result set". The result set
     * is represented as a list of tuples.
     */
    private OutputIR.Block buildInnerComprehension(final String viewName,
                                                   final MonoidComprehension comprehension,
                                                   @Nullable final GroupByQualifier groupByQualifier,
                                                   final boolean isConstraint, final ExprContext context) {
        final OutputIR.Block block = new OutputIR.Block(viewName);
        Preconditions.checkNotNull(comprehension.getHead());
        // Add a comment with the view name
        block.addHeader(
            CodeBlock.builder()
                     .add("\n")
                     .addStatement("/* $L view $L */", isConstraint ? "Constraint" : "Non-constraint", viewName)
                     .build()
        );

        // Extract the set of columns being selected in this view
        final AtomicInteger fieldIndex = new AtomicInteger();
        final List<ColumnIdentifier> headItemsList = getColumnsAccessed(comprehension, isConstraint);

        // Compute a string that represents the set of field accesses for the above columns
        context.enterScope(block);

        // Compute a string that represents the Java types corresponding to the headItemsStr
        final String headItemsListTupleGenericParameters = generateTupleGenericParameters(headItemsList);
        Preconditions.checkArgument(!headItemsListTupleGenericParameters.isEmpty(),
                "Generic parameters list for " + comprehension + " was empty");
        viewTupleTypeParameters.put(viewName, headItemsListTupleGenericParameters);

        final int tupleSize = headItemsList.size();
        final String resultSetNameStr = nonConstraintViewName(viewName);

        // For non-constraints, create a Map<> or a List<> to collect the result-set (depending on
        // whether the query is a group by or not)
        final OutputIR.Block resultSetDeclBlock = maybeAddMapOrListForResultSet(viewName, tupleSize,
                                                                     headItemsListTupleGenericParameters,
                                                                     resultSetNameStr, groupByQualifier, isConstraint);

        // Separate out qualifiers into variable and non-variable types.
        final QualifiersByType varQualifiers = new QualifiersByType();
        final QualifiersByType nonVarQualifiers = new QualifiersByType();
        populateQualifiersByVarType(comprehension, varQualifiers, nonVarQualifiers, true);

        // Start control flows to create nested for loops
        final OutputIR.Block forLoopsBlock = addNestedForLoops(viewName, nonVarQualifiers);

        // Filter out nested for loops using an if(predicate) statement
        context.enterScope(forLoopsBlock);

        // Identify head items to be accessed within loop
        final String headItemsStr = headItemsList.stream()
                .map(expr -> convertToFieldAccess(expr, viewName, fieldIndex, context))
                .collect(Collectors.joining(",\n    "));

        final OutputIR.Block nonVarFiltersBlock = maybeAddNonVarFilters(viewName, nonVarQualifiers,
                                                                        isConstraint, context);

        block.addChild(resultSetDeclBlock);
        block.addChild(forLoopsBlock);
        forLoopsBlock.addChild(nonVarFiltersBlock);
        if (!isConstraint // for simple constraints, we post constraints in this inner loop itself
           || (groupByQualifier != null) // for aggregate constraints, we populate a
                                         // result set and post constraints elsewhere
        ) {
            // If filter predicate is true, then retrieve expressions to collect into result set. Note, this does not
            // evaluate things like functions (sum etc.). These are not aggregated as part of the inner expressions.

            final OutputIR.Block resultSetAddBlock = addToResultSet(viewName, tupleSize, headItemsStr,
                                                               headItemsListTupleGenericParameters,
                                                               resultSetNameStr, groupByQualifier);
            forLoopsBlock.addChild(resultSetAddBlock);
        } else {
            final List<CodeBlock> addRowConstraintBlock = addRowConstraint(varQualifiers, nonVarQualifiers, context);
            addRowConstraintBlock.forEach(forLoopsBlock::addChild);
        }

        // Print debugging info
        context.leaveScope();
        context.leaveScope();
        return block;
    }

    private List<ColumnIdentifier> getColumnsAccessed(final MonoidComprehension comprehension,
                                                      final boolean isConstraint) {
        if (isConstraint) {
            final List<ColumnIdentifier> columnsAccessed = getColumnsAccessed(comprehension.getQualifiers());
            if (columnsAccessed.isEmpty()) {
                // There are constraints that are trivially true or false, wherein the predicate does not depend on
                // on any columns from the relations in the query. See the ModelTest.innerSubqueryCountTest
                // as an example. In such cases, we simply revert to pulling out all the head items for the outer query.
                Preconditions.checkArgument(comprehension.getHead() != null);
                return getColumnsAccessed(comprehension.getHead().getSelectExprs());
            }
            return columnsAccessed;
        } else {
            Preconditions.checkArgument(comprehension.getHead() != null);
            final List<ColumnIdentifier> columnsFromQualifiers  =
                    getColumnsAccessed(comprehension.getQualifiers());
            final List<ColumnIdentifier> columnsFromHead =
                    getColumnsAccessed(comprehension.getHead().getSelectExprs());
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
        final GetColumnIdentifiers visitor = new GetColumnIdentifiers();
        visitor.visit(expr);
        return visitor.getColumnIdentifiers();
    }

    private String convertToFieldAccess(final Expr expr, final String viewName, final AtomicInteger fieldIndex,
                                        final ExprContext context) {
        Preconditions.checkArgument(expr instanceof ColumnIdentifier);
        final String fieldName = updateFieldIndex(viewName, expr, fieldIndex);
        return exprToStr(expr, context) + " /* " + fieldName + " */";
    }

    private String updateFieldIndex(final String viewName, final Expr argument, final AtomicInteger counter) {
        final String fieldName = argument.getAlias().orElseGet(() -> {
                        if (argument instanceof ColumnIdentifier) {
                            return ((ColumnIdentifier) argument).getField().getName();
                        } else {
                            return GENERATED_FIELD_NAME_PREFIX + generatedFieldNameCounter.getAndIncrement();
                        }
                    }
                )
                .toUpperCase(Locale.US);
        viewToFieldIndex.computeIfAbsent(viewName.toUpperCase(Locale.US), (k) -> new HashMap<>())
                        .compute(fieldName, (k, v) -> counter.getAndIncrement());
        return fieldName;
    }

    private OutputIR.Block maybeAddMapOrListForResultSet(final String viewName, final int tupleSize,
                                                         final String headItemsListTupleGenericParameters,
                                                         final String viewRecords,
                                                         @Nullable final GroupByQualifier groupByQualifier,
                                                         final boolean isConstraint) {
        final OutputIR.Block block = new OutputIR.Block(viewName + "CreateResultSet");
        final TypeSpec tupleSpec = tupleGen.getTupleType(tupleSize);
        if (groupByQualifier != null) {
            // Create group by tuple
            final int numberOfGroupColumns = groupByQualifier.getColumnIdentifiers().size();
            final String groupByTupleGenericParameters =
                    generateTupleGenericParameters(groupByQualifier.getColumnIdentifiers());
            final TypeSpec groupTupleSpec = tupleGen.getTupleType(numberOfGroupColumns);
            viewGroupByTupleTypeParameters.put(viewName, groupByTupleGenericParameters);
            block.addHeader(CodeBlock.builder().addStatement("final Map<$N<$L>, $T<$N<$L>>> $L = new $T<>()",
                                        groupTupleSpec, groupByTupleGenericParameters,
                                        List.class, tupleSpec, headItemsListTupleGenericParameters,
                                        viewRecords, HashMap.class).build());
        } else {
            if (!isConstraint) {
                block.addHeader(CodeBlock.builder().addStatement("final $T<$N<$L>> $L = new $T<>()",
                                             List.class, tupleSpec, headItemsListTupleGenericParameters,
                                             viewRecords, ArrayList.class).build());
            }
        }
        return block;
    }

    private OutputIR.Block addNestedForLoops(final String viewName,
                                             final QualifiersByType nonVarQualifiers) {
        final List<CodeBlock> loopStatements = new ArrayList<>();
        nonVarQualifiers.tableRowGenerators.forEach(tr -> {
            final String tableName = tr.getTable().getName();
            final String tableNumRowsStr = tableNumRowsStr(tableName);
            final String iterStr = iterStr(tr.getTable().getAliasedName());
            loopStatements.add(CodeBlock.of("for (int $1L = 0; $1L < $2L; $1L++)", iterStr, tableNumRowsStr));
//            controlFlowsToPop.add(String.format("for (%s)", iterStr));
        });
        return new OutputIR.ForBlock(viewName, loopStatements);
    }

    private OutputIR.Block maybeAddNonVarFilters(final String viewName, final QualifiersByType nonVarQualifiers,
                                                 final boolean isConstraint, final ExprContext context) {
        final String joinPredicateStr = nonVarQualifiers.joinPredicates.stream()
                .map(expr -> exprToStr(expr, false, null, context))
                .collect(Collectors.joining(" \n    && "));
        // For non constraint views, where clauses are a filter criterion, whereas for constraint views, they
        // are the constraint itself.
        final String wherePredicateStr = isConstraint ? "" :
                                         nonVarQualifiers.wherePredicates.stream()
                                                .map(expr -> exprToStr(expr, false, null, context))
                                                .collect(Collectors.joining(" \n    && "));
        final String predicateStr = Stream.of(joinPredicateStr, wherePredicateStr)
                .filter(s -> !s.equals(""))
                .collect(Collectors.joining(" \n    && "));

        if (!predicateStr.isEmpty()) {
            // Add filter predicate if available
            final String nonVarFilter = CodeBlock.of("if (!($L))", predicateStr).toString();
            return new OutputIR.IfBlock(viewName + "nonVarFilter", nonVarFilter);
        }
        return new OutputIR.Block(viewName + "nonVarFilter");
    }

    private OutputIR.Block maybeAddNonVarAggregateFilters(final String viewName,
                                                          final QualifiersByType nonVarQualifiers,
                                                          final GroupContext groupContext,
                                                          final ExprContext exprContext) {
        final String predicateStr = nonVarQualifiers.aggregatePredicates.stream()
                .map(expr -> exprToStr(expr, false, groupContext, exprContext))
                .collect(Collectors.joining(" \n    && "));

        if (!predicateStr.isEmpty()) {
            // Add filter predicate if available
            final String nonVarFilter = CodeBlock.of("if ($L)", predicateStr).toString();
            return new OutputIR.IfBlock(viewName + "nonVarFilter", nonVarFilter);
        }
        return new OutputIR.Block(viewName + "nonVarFilter");
    }

    private OutputIR.Block addToResultSet(final String viewName, final int tupleSize,
                                          final String headItemsStr, final String headItemsListTupleGenericParameters,
                                          final String viewRecords, @Nullable final GroupByQualifier groupByQualifier) {
        final OutputIR.Block block = new OutputIR.Block(viewName + "AddToResultSet");
        // Create a tuple for the result set
        block.addChild(CodeBlock.builder().addStatement("final Tuple$1L<$2L> t = new Tuple$1L<>(\n    $3L\n    )",
                         tupleSize, headItemsListTupleGenericParameters, headItemsStr).build()
        );

        // Update result set
        if (groupByQualifier != null) {
            final int numberOfGroupByColumns = groupByQualifier.getColumnIdentifiers().size();
            // Comma separated list of field accesses to construct a group string
            final String groupString = groupByQualifier.getColumnIdentifiers().stream()
                    .map(e -> fieldNameStrWithIter(e.getTableName(), e.getField().getName()))
                    .collect(Collectors.joining(",     \n"));

            // Organize the collected tuples from the nested for loops by groupByTuple
            block.addChild(CodeBlock.builder()
                                     .addStatement("final Tuple$1L<$2L> groupByTuple = new Tuple$1L<>(\n    $3L\n    )",
                    numberOfGroupByColumns, Objects.requireNonNull(viewGroupByTupleTypeParameters.get(viewName)),
                    groupString).build());
            block.addChild(CodeBlock.builder()
                                      .addStatement("$L.computeIfAbsent(groupByTuple, (k) -> new $T<>()).add(t)",
                                                    viewRecords, ArrayList.class)
                                      .build());
        } else {
            block.addChild(CodeBlock.builder().addStatement("$L.add(t)", viewRecords).build());
        }
        return block;
    }

    private List<CodeBlock> addRowConstraint(final QualifiersByType varQualifiers,
                                  final QualifiersByType nonVarQualifiers, final ExprContext context) {
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
        varQualifiers.wherePredicates.forEach(e ->
                results.add(topLevelConstraint(e, joinPredicateStr, null, context)));
        nonVarQualifiers.wherePredicates.forEach(e ->
                results.add(topLevelConstraint(e, joinPredicateStr, null, context)));
        return results;
    }

    private List<CodeBlock> addAggregateConstraint(final QualifiersByType varQualifiers,
                                        final QualifiersByType nonVarQualifiers, final GroupContext groupContext,
                                        final ExprContext context) {
        final List<CodeBlock> results = new ArrayList<>();
        varQualifiers.aggregatePredicates.forEach(e ->
                results.add(topLevelConstraint(e, "", groupContext, context)));
        nonVarQualifiers.aggregatePredicates.forEach(e ->
                results.add(topLevelConstraint(e, "", groupContext, context)));
        return results;
    }

    private CodeBlock topLevelConstraint(final Expr expr, final String joinPredicateStr,
                                    @Nullable final GroupContext groupContext, final ExprContext context) {
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
                                @Nullable final GroupContext groupContext, final ExprContext context) {
        String exprStr = exprToStr(expr, true, groupContext, context);

        // Some special cases to handle booleans because the or-tools API does not convert well to booleans
        if (expr instanceof MonoidLiteral && ((MonoidLiteral) expr).getValue() instanceof Boolean) {
            exprStr = (Boolean) ((MonoidLiteral) expr).getValue() ? "1" : "0";
        }
        return inferType(expr).equals("IntVar") ? exprStr : String.format("o.toConst(%s)", exprStr);
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
            final Class recordType = Class.forName("org.jooq.Record" + table.getIRColumns().size());
            Preconditions.checkArgument(!tableToFieldToType.containsKey(table.getAliasedName()));
            final String recordTypeParameters = table.getIRColumns().entrySet().stream()
                    .map(e -> {
                        final String retVal = InferType.typeStringFromColumn(e.getValue());
                        // Tracks the type of each field.
                        tableToFieldToType.computeIfAbsent(table.getAliasedName(), (k) -> new HashMap<>())
                                          .putIfAbsent(e.getKey(), retVal);
                        return retVal;
                    }
                    ).collect(Collectors.joining(", "));

            if (table.isAliasedTable()) {
                continue;
            }
            output.addStatement("\n");
            output.addStatement("/* Table $S */", table.getName());
            // ...2) create a List<[RecordType]> to represent the table
            output.addStatement("final $T<$T<$L>> $L = (List<$T<$L>>) context.getTable($S).getCurrentData()",
                                 List.class, recordType, recordTypeParameters, tableNameStr(table.getName()),
                                 recordType, recordTypeParameters, table.getName());

            // ...3) for controllable fields, create a corresponding array of IntVars.
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
                    output.addStatement("\n");
                    output.addStatement("/* Primary key constraints for $L */", tableNameStr(table.getName()));

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
                        output.addStatement("\n");
                        output.addStatement("/* Foreign key constraints: $L.$L -> $L.$L */",
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

                        output.addStatement(snippet,
                               fieldIndex.get(), parent.getIRTable().getName(), parent.getName().toUpperCase(Locale.US),
                               toJavaClass(parent.getType()));
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

    private static String toJavaClass(final IRColumn.FieldType type) {
        switch (type) {
            case FLOAT:
                return "Float";
            case INT:
                return "Integer";
            case BOOL:
                return "Boolean";
            case STRING:
                return "String";
            default:
                throw new RuntimeException();
        }
    }

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

    private void addSolvePhase(final MethodSpec.Builder output, final IRContext context) {
        output.addCode("\n")
               .addComment("Start solving")
               .addStatement(printTime("Model creation"))
               .addStatement("final $1T solver = new $1T()", CpSolver.class)
               .addStatement("solver.getParameters().setLogSearchProgress(true)")
               .addStatement("solver.getParameters().setCpModelProbingLevel(0)")
               .addStatement("solver.getParameters().setNumSearchWorkers(4)")
               .addStatement("final $T status = solver.solve(model)", CpSolverStatus.class)
               .beginControlFlow("if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL)")
               .addStatement("final Map<IRTable, Result<? extends Record>> result = new $T<>()", HashMap.class)
               .addCode("final Object[] obj = new Object[1]; // Used to update controllable fields;\n");
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            final String tableName = table.getName();
            final Set<String> controllableColumns = table.getIRColumns().entrySet().stream()
                                                                   .filter(e -> e.getValue() // IRColumn
                                                                                 .isControllable())
                                                                   .map(Map.Entry::getKey)
                                                                   .collect(Collectors.toSet());
            // If the table does not have vars, we can return the original input as is
            if (controllableColumns.isEmpty()) {
                // Is an input table/view
                output.addStatement("result.put(context.getTable($1S), context.getTable($1S).getCurrentData())",
                                     tableName);
            } else {
                table.getIRColumns().forEach(
                    (name, field) -> {
                        // Else, we update the records corresponding to the table.
                        if (controllableColumns.contains(name)) {
                            final int i = intermediateViewCounter.incrementAndGet();
                            output.addStatement("final Result<? extends Record> tmp$L = " +
                                    "context.getTable($S).getCurrentData()", i, tableName);
                            output.beginControlFlow("for (int i = 0; i < $L; i++)",
                                    tableNumRowsStr(tableName));
                            if (field.getType().equals(IRColumn.FieldType.STRING)) {
                                output.addStatement("obj[0] = encoder.toStr(solver.value($L[i]))",
                                        fieldNameStr(tableName, field.getName()));
                            } else {
                                output.addStatement("obj[0] = solver.value($L[i])",
                                        fieldNameStr(tableName, field.getName()));
                            }
                            output.addStatement("tmp$L.get(i).from(obj, $S)", i, field.getName());
                            output.endControlFlow();
                            output.addStatement("result.put(context.getTable($S), tmp$L)", tableName, i);
                        }
                    }
                );
            }
        }
        output.addStatement("return result");
        output.endControlFlow();
        output.addStatement("throw new $T($S + status)", ModelException.class, "Could not solve ");
    }

    private static String tableNameStr(final String tableName) {
        return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName);
    }

    private static String fieldNameStr(final String tableName, final String fieldName) {
        return String.format("%s%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName),
                                     CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName));
    }

    private String fieldNameStrWithIter(final String tableName, final String fieldName) {
        return fieldNameStrWithIter(tableName, fieldName, iterStr(tableName));
    }

    private String fieldNameStrWithIter(final String tableName, final String fieldName, final String iterStr) {
        if (fieldName.contains("CONTROLLABLE")) {
            return String.format("%s[%s]", fieldNameStr(tableName, fieldName), iterStr);
        } else {
            if (viewToFieldIndex.containsKey(tableName)) {
                final int fieldIndex = viewToFieldIndex.get(tableName).get(fieldName);
                return String.format("%s.get(%s).value%s()", tableNameStr(tableName), iterStr, fieldIndex);
            } else {
                final String type = tableToFieldToType.get(tableName).get(fieldName);
                return String.format("%s.get(%s).get(\"%s\", %s.class)",
                        tableNameStr(tableName), iterStr, fieldName, type);
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
        final JavaFile javaFile = JavaFile.builder("org.dcm.backend", spec).build();
        LOG.info("Generating Java or-tools code: {}\n", javaFile.toString());

        // Compile Java code. This steps requires an SDK, and a JRE will not suffice
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        final Iterable<? extends JavaFileObject> compilationUnit = Collections.singleton(javaFile.toJavaFileObject());

        try {
            // Invoke SDK compiler
            final Path path = Path.of("/tmp/compilerOutput");
            final BufferedWriter fileWriter = Files.newBufferedWriter(path);
            final Boolean call = compiler.getTask(fileWriter, fileManager, null,
                    ImmutableList.of("-verbose", "-d", "/tmp/"), null, compilationUnit)
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
            final Class<?> cls = Class.forName(String.format("org.dcm.backend.%s", GENERATED_BACKEND_NAME), true,
                                               classLoader);
            final ConstructorAccess<?> access = ConstructorAccess.get(cls);
            generatedBackend = (IGeneratedBackend) access.newInstance();
            final StringWriter writer = new StringWriter();
            javaFile.writeTo(writer);
            return Collections.singletonList(writer.toString());
        } catch (final IOException | ClassNotFoundException e) {
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

    private String exprToStr(final Expr expr, final ExprContext context) {
        return exprToStr(expr, true, null, context);
    }

    private String exprToStr(final Expr expr, final boolean allowControllable,
                             @Nullable final GroupContext currentGroup, final ExprContext context) {
        final ExprToStrVisitor visitor = new ExprToStrVisitor(allowControllable, currentGroup, null);
        return Objects.requireNonNull(visitor.visit(expr, context));
    }

    private MonoidComprehension rewritePipeline(final MonoidComprehension comprehension) {
        return RewriteArity.apply(comprehension);
    }

    private <T extends Expr> String generateTupleGenericParameters(final List<T> exprs) {
        return exprs.stream().map(this::generateTupleGenericParameters)
                             .collect(Collectors.joining(", "));
    }

    private String generateTupleGenericParameters(final Expr expr) {
        return inferType(expr);
    }

    private void populateQualifiersByVarType(final MonoidComprehension comprehension, final QualifiersByType var,
                                             final QualifiersByType nonVar, final boolean skipAggregates) {
        comprehension.getQualifiers().forEach(
            q -> {
                final GetVarQualifiers.QualifiersList qualifiersList = GetVarQualifiers.apply(q, skipAggregates);
                qualifiersList.getNonVarQualifiers().forEach(nonVar::addQualifierByType);
                qualifiersList.getVarQualifiers().forEach(var::addQualifierByType);
            }
        );
        Preconditions.checkArgument(var.tableRowGenerators.isEmpty());
    }

    private class ExprToStrVisitor extends MonoidVisitor<String, ExprContext> {
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

        @Nullable
        @Override
        protected String visitMonoidFunction(final MonoidFunction node, @Nullable final ExprContext context) {
            final String vectorName = currentSubQueryContext == null ? currentGroupContext.groupViewName
                                                                     : currentSubQueryContext.subQueryName;
            assert context != null;
            // Functions always apply on a vector. We compute the arguments to the function, and in doing so,
            // add declarations to the corresponding for-loop that extracts the relevant columns/expressions from views.
            final OutputIR.Block forLoop = findLoopForVector(context.currentScope(), vectorName);
            if (node.getFunction().equals(MonoidFunction.Function.SUM)) {
                return maybeTranslateSumIntoScalarProduct(node.getArgument(), context.currentScope(), forLoop, context);
            }

            context.enterScope(forLoop);
            final String processedArgument = visit(node.getArgument(), context.withEnterFunctionContext());
            context.leaveScope();

            final String argumentType = inferType(node.getArgument());
            final boolean argumentIsIntVar = argumentType.equals("IntVar");

            final String listOfProcessedItem =
                    extractVectorFromView(processedArgument, context.currentScope(), forLoop, argumentType);
            String function = null;
            switch (node.getFunction()) {
                case SUM:
                    throw new IllegalStateException("Unreachable");
                case COUNT:
                    // In these cases, it is safe to replace count(argument) with sum(1)
                    if ((node.getArgument() instanceof MonoidLiteral ||
                         node.getArgument() instanceof ColumnIdentifier)) {
                        // TODO: another sign that groupContext/subQueryContext should be handled by ExprContext
                        //  and scopes
                        final String scanOver = currentSubQueryContext == null ?
                                                   "data" : currentSubQueryContext.subQueryName;
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
                    function = "allEqual";
                    break;
                case INCREASING:
                    context.currentScope().addChild(statement("o.increasing($L)", listOfProcessedItem));
                    return apply("model.newConstant(1)", context);
                case ALL_DIFFERENT:
                default:
                    throw new UnsupportedOperationException("Unsupported aggregate function " + node.getFunction());
            }
            Preconditions.checkNotNull(function);
            return CodeBlock.of("o.$L($L)", function, listOfProcessedItem).toString();
        }

        @Nullable
        @Override
        protected String visitExistsPredicate(final ExistsPredicate node, @Nullable final ExprContext context) {
            final String processedArgument = visit(node.getArgument(), context);
            return apply(String.format("o.exists(%s)", processedArgument), context);
        }

        @Nullable
        @Override
        protected String visitIsNullPredicate(final IsNullPredicate node, @Nullable final ExprContext context) {
            final String type = inferType(node.getArgument());
            final String processedArgument = visit(node.getArgument(), context);
            Preconditions.checkArgument(!type.equals("IntVar"));
            return apply(String.format("%s == null", processedArgument), context);
        }

        @Nullable
        @Override
        protected String visitIsNotNullPredicate(final IsNotNullPredicate node, @Nullable final ExprContext context) {
            final String type = inferType(node.getArgument());
            final String processedArgument = visit(node.getArgument(), context);
            Preconditions.checkArgument(!type.equals("IntVar"));
            return apply(String.format("%s != null", processedArgument), context);
        }

        @Nullable
        @Override
        protected String visitUnaryOperator(final UnaryOperator node, @Nullable final ExprContext context) {
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

        @Nullable
        @Override
        protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                      @Nullable final ExprContext context) {
            Preconditions.checkNotNull(context);
            final String left = Objects.requireNonNull(visit(node.getLeft(), context),
                                                       "Expr was null: " + node.getLeft());
            final String right = Objects.requireNonNull(visit(node.getRight(), context),
                                                        "Expr was null: " + node.getRight());
            final BinaryOperatorPredicate.Operator op = node.getOperator();
            final String leftType = inferType(node.getLeft());
            final String rightType = inferType(node.getRight());

            if (leftType.equals("IntVar") || rightType.equals("IntVar")) {
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

        @Nullable
        @Override
        protected String visitColumnIdentifier(final ColumnIdentifier node, @Nullable final ExprContext context) {
            Preconditions.checkNotNull(context);
            // If we are evaluating a group-by comprehension, then column accesses that happen outside the context
            // of an aggregation function must refer to the grouping column, not the inner tuple being iterated over.
            if (!context.isFunctionContext() && currentGroupContext != null) {
                final String tableName = node.getTableName();
                final String fieldName = node.getField().getName();
                int columnNumber = 0;
                for (final ColumnIdentifier ci: currentGroupContext.qualifier.getColumnIdentifiers()) {
                    if (ci.getTableName().equalsIgnoreCase(tableName)
                            && ci.getField().getName().equalsIgnoreCase(fieldName)) {
                        return apply(String.format("group.value%s()", columnNumber), context);
                    }
                    columnNumber++;
                }
                throw new UnsupportedOperationException("Could not find group-by column " + node);
            }

            // TODO: The next two blocks can both simultaenously be true. This can happen when we are within
            //  a subquery and a group-by context at the same time. We need a cleaner way to distinguish what
            //  scope to be searching for the indices.
            // Sub-queries also use an intermediate view, and we again need an indirection from column names to indices
            if (context.isFunctionContext() && currentSubQueryContext != null) {
                final String tempTableName = currentSubQueryContext.subQueryName.toUpperCase(Locale.US);
                final int fieldIndex = viewToFieldIndex.get(tempTableName).get(node.getField().getName());
                return apply(String.format("t.value%s()", fieldIndex), context);
            }

            // Within a group-by, we refer to values from the intermediate group by table. This involves an
            // indirection from columns to tuple indices
            if (context.isFunctionContext() && currentGroupContext != null) {
                final String tempTableName = currentGroupContext.tempTableName.toUpperCase(Locale.US);
                final int fieldIndex = viewToFieldIndex.get(tempTableName).get(node.getField().getName());
                return apply(String.format("t.value%s()", fieldIndex), context);
            }

            final String tableName = node.getField().getIRTable().getName();
            final String fieldName = node.getField().getName();
            final String iterStr = iterStr(node.getTableName());
            if (!allowControllable && fieldName.contains("CONTROLLABLE")) {
                throw new UnsupportedOperationException("Controllable variables not allowed in predicates");
            }
            return apply(fieldNameStrWithIter(tableName, fieldName, iterStr), context);
        }

        @Nullable
        @Override
        protected String visitMonoidLiteral(final MonoidLiteral node, @Nullable final ExprContext context) {
            if (node.getValue() instanceof String) {
                return node.getValue().toString().replace("'", "\"");
            } else {
                return node.getValue().toString();
            }
        }

        @Nullable
        @Override
        protected String visitMonoidComprehension(final MonoidComprehension node,
                                                  @Nullable final ExprContext context) {
            // We are in a subquery.
            final String newSubqueryName = SUBQUERY_NAME_PREFIX + subqueryCounter.incrementAndGet();
            final OutputIR.Block currentBlock = context.currentScope();
            final OutputIR.Block subQueryBlock = addView(newSubqueryName, node, false, context);
            Preconditions.checkNotNull(node.getHead());
            Preconditions.checkArgument(node.getHead().getSelectExprs().size() == 1);
            final ExprToStrVisitor innerVisitor =
                    new ExprToStrVisitor(allowControllable, currentGroupContext,
                                         new SubQueryContext(newSubqueryName));
            Preconditions.checkArgument(node.getHead().getSelectExprs().size() == 1);
            final Expr headSelectItem = node.getHead().getSelectExprs().get(0);

            final ContainsMonoidFunction visitor = new ContainsMonoidFunction();
            visitor.visit(headSelectItem);
            final boolean headSelectItemContainsMonoidFunction = visitor.getFound();

            // If the head contains a function, then this is a scalar subquery
            final ExprContext newCtx = Objects.requireNonNull(context).withEnterFunctionContext();
            if (headSelectItemContainsMonoidFunction) {
                newCtx.enterScope(subQueryBlock);
                currentBlock.addChild(subQueryBlock);
                final String ret = apply(Objects.requireNonNull(innerVisitor.visit(headSelectItem, newCtx)), context);
                newCtx.leaveScope();
                return ret;
            } else {
                // Else, treat the result as a vector
                newCtx.enterScope(findForLoopBlockByName(subQueryBlock, newSubqueryName));
                final String processedHeadItem = innerVisitor.visit(node.getHead().getSelectExprs().get(0), newCtx);
                final String type = inferType(node.getHead().getSelectExprs().get(0));
                final String listName =
                        extractVectorFromView(processedHeadItem, subQueryBlock, newSubqueryName, type);
                currentBlock.addChild(subQueryBlock);
                newCtx.leaveScope();
                return apply(listName, subQueryBlock, context);
            }
        }

        @Nullable
        @Override
        protected String visitGroupByComprehension(final GroupByComprehension node,
                                                   @Nullable final ExprContext context) {
            // We are in a subquery.
            final String newSubqueryName = SUBQUERY_NAME_PREFIX + subqueryCounter.incrementAndGet();
            final OutputIR.Block currentBlock = context.currentScope();
            final OutputIR.Block subQueryBlock = addView(newSubqueryName, node, false, context);
            Preconditions.checkNotNull(node.getComprehension().getHead());
            Preconditions.checkArgument(node.getComprehension().getHead().getSelectExprs().size() == 1);
            final ExprToStrVisitor innerVisitor =
                    new ExprToStrVisitor(allowControllable, currentGroupContext,
                            new SubQueryContext(newSubqueryName));
            Preconditions.checkArgument(node.getComprehension().getHead().getSelectExprs().size() == 1);
            final Expr headSelectItem = node.getComprehension().getHead().getSelectExprs().get(0);

            // if scalar subquery
            final ExprContext newCtx = Objects.requireNonNull(context).withEnterFunctionContext();
            if (headSelectItem instanceof MonoidFunction) {
                newCtx.enterScope(subQueryBlock);
                currentBlock.addChild(subQueryBlock);
                final String ret = apply(Objects.requireNonNull(innerVisitor.visit(headSelectItem, newCtx)), context);
                newCtx.leaveScope();
                return ret;
            } else {
                newCtx.enterScope(findForLoopBlockByName(subQueryBlock, newSubqueryName));
                final String processedHeadItem =
                    Objects.requireNonNull(innerVisitor.visit(node.getComprehension().getHead().getSelectExprs().get(0),
                                           newCtx));
                final String type = inferType(node.getComprehension().getHead().getSelectExprs().get(0));
                final String listName =
                        extractVectorFromView(processedHeadItem, subQueryBlock, newSubqueryName, type);
                currentBlock.addChild(subQueryBlock);
                newCtx.leaveScope();
                // Treat as a vector
                return apply(listName, subQueryBlock, context);
            }
        }

        protected String apply(final String result, final ExprContext context) {
            return context.declareVariable(result);
        }

        protected String apply(final String result, final OutputIR.Block block, final ExprContext context) {
            return context.declareVariable(result, block);
        }


        private String extractVectorFromView(final String processedHeadItem, final OutputIR.Block viewBlock,
                                                    final String viewName, final String type) {
            final OutputIR.Block forLoop = findForLoopBlockByName(viewBlock, viewName);
            return extractVectorFromView(processedHeadItem, viewBlock, forLoop, type);
        }

        private OutputIR.Block findForLoopBlockByName(final OutputIR.Block currentBlock,
                                                             final String childBlockName) {
            final List<OutputIR.BlockExpr> childBlocks = currentBlock.getForLoopsByName(childBlockName);
            assert childBlocks.size() == 1 && childBlocks.get(0) instanceof OutputIR.ForBlock;
            return ( OutputIR.ForBlock) childBlocks.get(0);
        }

        private OutputIR.Block findLoopForVector(final OutputIR.Block currentBlock, final String viewName) {
            final List<OutputIR.BlockExpr> childBlocks = currentBlock.getForLoopsByName(viewName);
            assert childBlocks.size() == 1  && childBlocks.get(0) instanceof OutputIR.ForBlock;;
            return (OutputIR.ForBlock) childBlocks.get(0);
        }

        private String extractVectorFromView(final String processedHeadItem, final OutputIR.Block outerBlock,
                                                    final OutputIR.Block innerBlock, final String type) {
            final String listName = "listOf" + processedHeadItem;
            final boolean wasAdded = outerBlock.addHeader(statement("final List<$L> listOf$L = new $T<>()",
                    type, processedHeadItem, ArrayList.class));
            if (wasAdded) {
                innerBlock.addChild(statement("$L.add($L)", listName, processedHeadItem));
            }
            return listName;
        }

        private String maybeTranslateSumIntoScalarProduct(final Expr node, final OutputIR.Block outerBlock,
                                                          final OutputIR.Block forLoop, final ExprContext context) {
            if (node instanceof BinaryOperatorPredicate) {
                final BinaryOperatorPredicate operation = (BinaryOperatorPredicate) node;
                final BinaryOperatorPredicate.Operator op = operation.getOperator();
                final Expr left = operation.getLeft();
                final Expr right = operation.getRight();
                final String leftType = inferType(left);
                final String rightType = inferType(right);
                if (op.equals(BinaryOperatorPredicate.Operator.MULTIPLY)) {
                    if (leftType.equals("IntVar") && !rightType.equals("IntVar")) {
                        return createTermsForScalarProduct(left, right, context, outerBlock, forLoop, rightType);
                    }
                    if (rightType.equals("IntVar") && !leftType.equals("IntVar")) {
                        return createTermsForScalarProduct(right, left, context, outerBlock, forLoop, leftType);
                    }
                }
            }
            // regular sum
            context.enterScope(forLoop);
            final String processedArgument = Objects.requireNonNull(visit(node, context.withEnterFunctionContext()));
            context.leaveScope();
            final String argumentType = inferType(node);
            final String function = argumentType.equals("IntVar") ? "sumV" : "sum";
            final String listOfProcessedItem =
                    extractVectorFromView(processedArgument, context.currentScope(), forLoop, argumentType);
            return CodeBlock.of("o.$L($L)", function, listOfProcessedItem).toString();
        }

        private String createTermsForScalarProduct(final Expr variables, final Expr coefficients,
                                                   final ExprContext context, final OutputIR.Block outerBlock,
                                                   final OutputIR.Block forLoop,
                                                   final String coefficientsType) {
            context.enterScope(forLoop);
            final String variablesItem = Objects.requireNonNull(visit(variables, context.withEnterFunctionContext()));
            final String coefficientsItem = Objects.requireNonNull(visit(coefficients,
                                                                         context.withEnterFunctionContext()));
            context.leaveScope();
            final String listOfVariablesItem =
                    extractVectorFromView(variablesItem, outerBlock, forLoop, "IntVar");
            final String listOfCoefficientsItem =
                    extractVectorFromView(coefficientsItem, outerBlock, forLoop, coefficientsType);
            return CodeBlock.of("o.scalProd($L, $L)", listOfVariablesItem, listOfCoefficientsItem).toString();
        }
    }

    private static class ContainsMonoidFunction extends MonoidVisitor<Boolean, Void> {
        boolean found = false;

        @Nullable
        @Override
        protected Boolean visitMonoidFunction(final MonoidFunction node, @Nullable final Void context) {
            found = true;
            return super.visitMonoidFunction(node, context);
        }

        boolean getFound() {
            return found;
        }
    }

    private static class GroupContext {
        private final GroupByQualifier qualifier;
        private final String tempTableName;
        private final String groupViewName;

        private GroupContext(final GroupByQualifier qualifier, final String tempTableName, final String groupViewName) {
            this.qualifier = qualifier;
            this.tempTableName = tempTableName;
            this.groupViewName = groupViewName;
        }
    }

    private static class SubQueryContext {
        private final String subQueryName;

        private SubQueryContext(final String subQueryName) {
            this.subQueryName = subQueryName;
        }
    }

    private static class QualifiersByType {
        private final List<BinaryOperatorPredicate> wherePredicates = new ArrayList<>();
        private final List<JoinPredicate> joinPredicates = new ArrayList<>();
        private final List<BinaryOperatorPredicateWithAggregate> aggregatePredicates = new ArrayList<>();
        private final List<TableRowGenerator> tableRowGenerators = new ArrayList<>();

        private void addQualifierByType(final Qualifier q) {
            if (q instanceof BinaryOperatorPredicateWithAggregate) {
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
        return CodeBlock.of("System.out.println(\"$L: we are at \" + (System.nanoTime() - startTime));", event);
    }

    private String inferType(final Expr expr) {
        return InferType.forExpr(expr, viewTupleTypeParameters);
    }

    private String getTempViewName() {
        return  "tmp" + intermediateViewCounter.getAndIncrement();
    }

    private static class ExprContext {
        private final Deque<OutputIR.Block> scopeStack;
        private final boolean isFunctionContext;

        private ExprContext(final Deque<OutputIR.Block> declarations, final boolean isFunctionContext) {
            this.scopeStack = declarations;
            this.isFunctionContext = isFunctionContext;
        }

        private ExprContext(final boolean isFunctionContext) {
            this(new ArrayDeque<>(), isFunctionContext);
        }

        ExprContext withEnterFunctionContext() {
            final Deque<OutputIR.Block> stackCopy = new ArrayDeque<>(scopeStack);
            return new ExprContext(stackCopy, true);
        }

        boolean isFunctionContext() {
            return isFunctionContext;
        }

        void enterScope(final OutputIR.Block block) {
            scopeStack.addLast(block);
        }

        OutputIR.Block currentScope() {
            return Objects.requireNonNull(scopeStack.getLast());
        }

        OutputIR.Block leaveScope() {
            return scopeStack.removeLast();
        }

        String declareVariable(final String expression) {
            for (final OutputIR.Block block: scopeStack) {
                if (block.hasDeclaration(expression)) {
                    return block.getDeclaredName(expression);
                }
            }
            return scopeStack.getLast().declare(expression);
        }

        String declareVariable(final String expression, final OutputIR.Block block) {
            return block.declare(expression);
        }
    }

    private static CodeBlock statement(final String format, final Object... args) {
        return CodeBlock.builder().addStatement(format, args).build();
    }
}