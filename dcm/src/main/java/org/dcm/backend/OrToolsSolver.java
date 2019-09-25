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
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.util.Domain;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
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
import org.dcm.compiler.monoid.Head;
import org.dcm.compiler.monoid.JoinPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.MonoidVisitor;
import org.dcm.compiler.monoid.Qualifier;
import org.dcm.compiler.monoid.TableRowGenerator;

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
        System.loadLibrary("jniortools");
    }

    @Nullable private IGeneratedBackend generatedBackend; // = new ExampleBackend();
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
                    addView(output, name, rewrittenComprehension, false);
                });
        constraintViews
                .forEach((name, comprehension) -> {
                    final MonoidComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    addView(output, name, rewrittenComprehension, true);
                });

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

    private void addView(final MethodSpec.Builder output, final String viewName,
                         final MonoidComprehension comprehension, final boolean isConstraint) {
        addView(output, viewName, comprehension, isConstraint, false);
    }

    private void addView(final MethodSpec.Builder output, final String viewName,
                         final MonoidComprehension comprehension, final boolean isConstraint,
                         final boolean isSubquery) {
        if (comprehension instanceof GroupByComprehension) {
            final GroupByComprehension groupByComprehension = (GroupByComprehension) comprehension;
            final MonoidComprehension inner = groupByComprehension.getComprehension();
            final GroupByQualifier groupByQualifier = groupByComprehension.getGroupByQualifier();

            // Rewrite inner comprehension using the following rule:
            // [ [blah - sum(X) | <....>] | G]
            //                     rows
            //

            // We create an intermediate view that extracts groups and returns a List<Tuple> per group
            final String intermediateView = getTempViewName();
            buildInnerComprehension(output, intermediateView, inner, groupByQualifier, isConstraint);

            // We now construct the actual result set that hosts the aggregated tuples by group. This is done
            // in two steps...
            final int groupByQualifiersSize = groupByQualifier.getColumnIdentifiers().size();
            final String groupByTupleTypeParameters = viewGroupByTupleTypeParameters.get(intermediateView);
            final String headItemsTupleTypeParamters = viewTupleTypeParameters.get(intermediateView);
            assert inner.getHead() != null;
            final int innerTupleSize = viewToFieldIndex.get(intermediateView.toUpperCase(Locale.US)).size();

            // (1) Create the result set
            output.addCode("\n");
            output.addStatement(printTime("Group-by intermediate view"));
            output.addComment("$L view $L", isConstraint ? "Constraint" : "Non-constraint",
                                            tableNameStr(viewName));

            if (!isConstraint) {
                final int selectExprSize = inner.getHead().getSelectExprs().size();
                final TypeSpec typeSpec = tupleGen.getTupleType(selectExprSize);
                final String viewTupleGenericParameters =
                        generateTupleGenericParameters(inner.getHead().getSelectExprs());
                viewTupleTypeParameters.put(tableNameStr(viewName), viewTupleGenericParameters);
                output.addStatement("final $T<$N<$L>> $L = new $T<>($L.size())", List.class, typeSpec,
                        viewTupleGenericParameters, tableNameStr(viewName), ArrayList.class, intermediateView);
            }

            // (2) Loop over the result set collected from the inner comprehension
            final ArrayDeque<String> controlFlowsToPop = new ArrayDeque<>();
            output.beginControlFlow("for (final $T<Tuple$L<$L>, List<Tuple$L<$L>>> entry: $L.entrySet())",
                    Map.Entry.class, groupByQualifiersSize, groupByTupleTypeParameters, innerTupleSize,
                    headItemsTupleTypeParamters, intermediateView);
            controlFlowsToPop.add("group-by-for-loop");
            output.addStatement("final Tuple$L<$L> group = entry.getKey()", groupByQualifiersSize,
                    groupByTupleTypeParameters);
            output.addStatement("final List<Tuple$L<$L>> data = entry.getValue()", innerTupleSize,
                    headItemsTupleTypeParamters);

            // (3) Filter if necessary
            final QualifiersByType varQualifiers = new QualifiersByType();
            final QualifiersByType nonVarQualifiers = new QualifiersByType();
            populateQualifiersByVarType(inner, varQualifiers, nonVarQualifiers, false);
            maybeAddNonVarAggregateFilters(output, nonVarQualifiers, controlFlowsToPop);

            // If this is not a constraint, we simply add a to a result set
            if (!isConstraint) {
                final TypeSpec typeSpec = tupleGen.getTupleType(inner.getHead().getSelectExprs().size());
                final String viewTupleGenericParameters =
                        generateTupleGenericParameters(inner.getHead().getSelectExprs());
                output.addCode("final $1N<$2L> res = new $1N<>(", typeSpec, viewTupleGenericParameters);
                int numSelectExprs = inner.getHead().getSelectExprs().size();
                for (final Expr expr : inner.getHead().getSelectExprs()) {
                    final String result =
                            exprToStr(output, expr, true, new GroupContext(groupByQualifier, intermediateView));
                    if (result.contains("$1T")) {
                        output.addCode(result, Collectors.class);
                    } else {
                        output.addCode(result);
                    }

                    if (numSelectExprs > 1) {
                        numSelectExprs--;
                        output.addCode(","); // Separate tuple entries by a comma
                    }
                }
                output.addCode(");\n");
                output.addStatement("$L.add(res)", tableNameStr(viewName));
            }
            else  {
                // If this is a constraint, we translate having clauses into a constraint statement
                assert !varQualifiers.aggregatePredicates.isEmpty();
                addAggregateConstraint(output, varQualifiers, nonVarQualifiers,
                                       new GroupContext(groupByQualifier, intermediateView));
            }

            controlFlowsToPop.forEach(s -> output.endControlFlow());
            output.addStatement(printTime("Group-by final view"));
            return;
        } else if (isSubquery) {
            if (comprehension.getHead() != null && comprehension.getHead().getSelectExprs().size() == 1 &&
                    hasAggregate(comprehension.getHead().getSelectExprs().get(0))) {
                final String intermediateViewName = getTempViewName();
                buildInnerComprehension(output, intermediateViewName, comprehension, null, isConstraint);
                final MonoidFunction function = (MonoidFunction) comprehension.getHead().getSelectExprs().get(0);
                output.addStatement("final Integer $L = o.$LV($L.stream().map(Tuple1::value0)" +
                                "\n                                 .mapToLong(encoder::toLong).toArray())",
                                     viewName, function.getFunctionName(), intermediateViewName);
                return;
            }
        }
        buildInnerComprehension(output, viewName, comprehension, null, isConstraint);
    }

    /**
     * Converts a comprehension into a set of nested for loops that return a "result set". The result set
     * is represented as a list of tuples.
     */
    private void buildInnerComprehension(final MethodSpec.Builder output, final String viewName,
                                         final MonoidComprehension comprehension,
                                         @Nullable final GroupByQualifier groupByQualifier,
                                         final boolean isConstraint) {
        Preconditions.checkNotNull(comprehension.getHead());
        // Add a comment with the view name
        output.addCode("\n").addComment("$L view $L", isConstraint ? "Constraint" : "Non-constraint", viewName);

        // Extract the set of columns being selected in this view
        final AtomicInteger fieldIndex = new AtomicInteger();
        final List<ColumnIdentifier> headItemsList = getColumnsAccessed(comprehension, isConstraint);

        // Compute a string that represents the set of field accesses for the above columns
        final String headItemsStr = headItemsList.stream()
                .map(expr -> convertToFieldAccess(output, expr, viewName, fieldIndex))
                .collect(Collectors.joining(",\n    "));

        // Compute a string that represents the Java types corresponding to the headItemsStr
        final String headItemsListTupleGenericParameters = generateTupleGenericParameters(headItemsList);
        Preconditions.checkArgument(!headItemsListTupleGenericParameters.isEmpty(),
                "Generic parameters list for " + comprehension + " was empty");
        viewTupleTypeParameters.put(viewName, headItemsListTupleGenericParameters);

        final int tupleSize = headItemsList.size();
        final String resultSetNameStr = nonConstraintViewName(viewName);
        final ArrayDeque<String> controlFlowsToPop = new ArrayDeque<>();

        // For non-constraints, create a Map<> or a List<> to collect the result-set (depending on
        // whether the query is a group by or not)
        maybeAddMapOrListForResultSet(output, viewName, tupleSize, headItemsListTupleGenericParameters,
                                      resultSetNameStr, groupByQualifier, isConstraint);

        // Separate out qualifiers into variable and non-variable types.
        final QualifiersByType varQualifiers = new QualifiersByType();
        final QualifiersByType nonVarQualifiers = new QualifiersByType();
        populateQualifiersByVarType(comprehension, varQualifiers, nonVarQualifiers, true);

        // Start control flows to create nested for loops
        addNestedForLoops(output, nonVarQualifiers, controlFlowsToPop);

        // Filter out nested for loops using an if(predicate) statement
        maybeAddNonVarFilters(output, nonVarQualifiers, controlFlowsToPop, isConstraint);

        if (!isConstraint // for simple constraints, we post constraints in this inner loop itself
           || (groupByQualifier != null) // for aggregate constraints, we populate a
                                         // result set and post constraints elsewhere
        ) {
            // If filter predicate is true, then retrieve expressions to collect into result set. Note, this does not
            // evaluate things like functions (sum etc.). These are not aggregated as part of the inner expressions.
            addToResultSet(output, viewName, tupleSize, headItemsStr, headItemsListTupleGenericParameters,
                    resultSetNameStr, groupByQualifier);
        } else {
            addRowConstraint(output, varQualifiers, nonVarQualifiers);
        }

        // Pop all control flows (nested for loops and if statement)
        controlFlowsToPop.forEach(
                e -> output.endControlFlow()
        );
        // Print debugging info
        // output.addStatement("$T.out.println($N)", System.class, viewRecords);
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
            return getColumnsAccessed(comprehension.getHead().getSelectExprs());
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

    private String convertToFieldAccess(final MethodSpec.Builder output, final Expr expr,
                                        final String viewName, final AtomicInteger fieldIndex) {
        Preconditions.checkArgument(expr instanceof ColumnIdentifier);
        final String fieldName = updateFieldIndex(viewName, expr, fieldIndex);
        return exprToStr(output, expr)  + " /* " + fieldName + " */";
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
                        .computeIfAbsent(fieldName, (k) -> counter.getAndIncrement());
        return fieldName;
    }

    private void maybeAddMapOrListForResultSet(final MethodSpec.Builder output, final String viewName,
                                               final int tupleSize, final String headItemsListTupleGenericParameters,
                                               final String viewRecords,
                                               @Nullable final GroupByQualifier groupByQualifier,
                                               final boolean isConstraint) {
        final TypeSpec tupleSpec = tupleGen.getTupleType(tupleSize);
        if (groupByQualifier != null) {
            // Create group by tuple
            final int numberOfGroupColumns = groupByQualifier.getColumnIdentifiers().size();
            final String groupByTupleGenericParameters =
                    generateTupleGenericParameters(groupByQualifier.getColumnIdentifiers());
            final TypeSpec groupTupleSpec = tupleGen.getTupleType(numberOfGroupColumns);
            viewGroupByTupleTypeParameters.put(viewName, groupByTupleGenericParameters);
            output.addStatement("final Map<$N<$L>, $T<$N<$L>>> $L = new $T<>()",
                    groupTupleSpec, groupByTupleGenericParameters,
                    List.class, tupleSpec, headItemsListTupleGenericParameters,
                    viewRecords, HashMap.class);
        } else {
            if (!isConstraint) {
                output.addStatement("final $T<$N<$L>> $L = new $T<>()",
                        List.class, tupleSpec, headItemsListTupleGenericParameters, viewRecords, ArrayList.class);
            }
        }
    }

    private void addNestedForLoops(final MethodSpec.Builder output, final QualifiersByType nonVarQualifiers,
                                   final ArrayDeque<String> controlFlowsToPop) {
        nonVarQualifiers.tableRowGenerators.forEach(tr -> {
            final String tableName = tr.getTable().getName();
            final String tableNumRowsStr = tableNumRowsStr(tableName);
            final String iterStr = iterStr(tr.getTable().getAliasedName());
            output.beginControlFlow("for (int $1L = 0; $1L < $2L; $1L++)", iterStr, tableNumRowsStr);
            controlFlowsToPop.add(String.format("for (%s)", iterStr));
        });
    }

    private void maybeAddNonVarFilters(final MethodSpec.Builder output, final QualifiersByType nonVarQualifiers,
                                       final ArrayDeque<String> controlFlowsToPop, final boolean isConstraint) {
        final String joinPredicateStr = nonVarQualifiers.joinPredicates.stream()
                .map(expr -> exprToStr(output, expr, false, null))
                .collect(Collectors.joining(" \n    && "));
        // For non constraint views, where clauses are a filter criterion, whereas for constraint views, they
        // are the constraint itself.
        final String wherePredicateStr = isConstraint ? "" :
                                         nonVarQualifiers.wherePredicates.stream()
                                                .map(expr -> exprToStr(output, expr, false, null))
                                                .collect(Collectors.joining(" \n    && "));
        final String predicateStr = Stream.of(joinPredicateStr, wherePredicateStr)
                .filter(s -> !s.equals(""))
                .collect(Collectors.joining(" \n    && "));

        if (!predicateStr.isEmpty()) {
            // Add filter predicate if available
            output.beginControlFlow("if ($L)", predicateStr);
            controlFlowsToPop.add("if (" + predicateStr + ")");
        }
    }

    private void maybeAddNonVarAggregateFilters(final MethodSpec.Builder output,
                                       final QualifiersByType nonVarQualifiers,
                                       final ArrayDeque<String> controlFlowsToPop) {
        final String predicateStr = nonVarQualifiers.aggregatePredicates.stream()
                .map(expr -> exprToStr(output, expr, false, null))
                .collect(Collectors.joining(" \n    && "));

        if (!predicateStr.isEmpty()) {
            // Add filter predicate if available
            output.beginControlFlow("if ($L)", predicateStr);
            controlFlowsToPop.add("if (" + predicateStr + ")");
        }
    }

    private void addToResultSet(final MethodSpec.Builder output, final String viewName, final int tupleSize,
                                final String headItemsStr, final String headItemsListTupleGenericParameters,
                                final String viewRecords, @Nullable final GroupByQualifier groupByQualifier) {
        // Create a tuple for the result set
        output.addStatement("final Tuple$1L<$2L> tuple = new Tuple$1L<>(\n    $3L\n    )",
                tupleSize, headItemsListTupleGenericParameters, headItemsStr);

        // Update result set
        if (groupByQualifier != null) {
            final int numberOfGroupByColumns = groupByQualifier.getColumnIdentifiers().size();
            // Comma separated list of field accesses to construct a group string
            final String groupString = groupByQualifier.getColumnIdentifiers().stream()
                    .map(e -> fieldNameStrWithIter(e.getTableName(), e.getField().getName()))
                    .collect(Collectors.joining(",     \n"));

            // Organize the collected tuples from the nested for loops by groupByTuple
            output.addStatement("final Tuple$1L<$2L> groupByTuple = new Tuple$1L<>(\n    $3L\n    )",
                    numberOfGroupByColumns, Objects.requireNonNull(viewGroupByTupleTypeParameters.get(viewName)),
                    groupString);
            output.addStatement("$L.computeIfAbsent(groupByTuple, (k) -> new $T<>()).add(tuple)",
                    viewRecords, ArrayList.class);
        } else {
            output.addStatement("$L.add(tuple)", viewRecords);
        }
    }

    private void addRowConstraint(final MethodSpec.Builder output, final QualifiersByType varQualifiers,
                                  final QualifiersByType nonVarQualifiers) {
        final List<String> varJoinPredicateStr = varQualifiers.joinPredicates.stream()
                .map(expr -> exprToStr(output, expr, true, null))
                .collect(Collectors.toList());
        Preconditions.checkArgument(varJoinPredicateStr.isEmpty()); // TODO: this should be an implication below
        varQualifiers.wherePredicates.forEach(e -> topLevelConstraint(output, e, null));
        nonVarQualifiers.wherePredicates.forEach(e -> topLevelConstraint(output, e, null));
    }

    private void addAggregateConstraint(final MethodSpec.Builder output, final QualifiersByType varQualifiers,
                                        final QualifiersByType nonVarQualifiers, final GroupContext groupContext) {
        varQualifiers.aggregatePredicates.forEach(e -> topLevelConstraint(output, e, groupContext));
        nonVarQualifiers.aggregatePredicates.forEach(e -> topLevelConstraint(output, e, groupContext));
    }

    private void topLevelConstraint(final MethodSpec.Builder output, final Expr expr,
                                    @Nullable final GroupContext groupContext) {
        Preconditions.checkArgument(expr instanceof BinaryOperatorPredicate);
        final BinaryOperatorPredicate predicate = (BinaryOperatorPredicate) expr;
        final Expr left = predicate.getLeft();
        final Expr right = predicate.getRight();
        final String op = predicate.getOperator();
        // TODO: May not be necessary to wrap the 'right' parameters for the below constraints
        String statement;
        switch (op) {
            case "in":
                final String subqueryStr = exprToStr(output, right, true, groupContext);
                final String block = "final $T domain = $T.fromValues($L.stream()" +
                                             "\n                                .map(Tuple1::value0)" +
                                             "\n                                .mapToLong(encoder::toLong).toArray())";
                output.addStatement(block, Domain.class, Domain.class, nonConstraintViewName(subqueryStr));
                output.addStatement(String.format("model.addLinearExpressionInDomain(%s, domain)",
                                    maybeWrapped(output, left, groupContext)));
                return;
            case "==":
                statement = String.format("model.addEquality(%s, %s)", maybeWrapped(output, left, groupContext),
                                                                       maybeWrapped(output, right, groupContext));
                break;
            case "!=":
                statement = String.format("model.addDifferent((%s, %s)", maybeWrapped(output, left, groupContext),
                                                                         maybeWrapped(output, right, groupContext));
                break;
            case "<=":
                statement = String.format("model.addLessOrEqual(%s, %s)", maybeWrapped(output, left, groupContext),
                                                                          maybeWrapped(output, right, groupContext));
                break;
            case "<":
                statement = String.format("model.addLessThan(%s, %s)", maybeWrapped(output, left, groupContext),
                                                                       maybeWrapped(output, right, groupContext));
                break;
            case ">=":
                statement = String.format("model.addGreaterOrEqual(%s, %s)", maybeWrapped(output, left, groupContext),
                                                                             maybeWrapped(output, right, groupContext));
                break;
            case ">":
                statement = String.format("model.addGreaterThan(%s, %s)", maybeWrapped(output, left, groupContext),
                                                                          maybeWrapped(output, right, groupContext));
                break;
            case "\\/":
                statement = String.format("model.addBoolOr(%s, %s)", maybeWrapped(output, left, groupContext),
                        maybeWrapped(output, right, groupContext));
                break;
            default:
                throw new UnsupportedOperationException();
        }
        if (statement.contains("$1T")) {
            output.addStatement(statement, Collectors.class);
        } else {
            output.addStatement(statement);
        }
    }

    /**
     * Wrap constants 'x' in model.newConstant(x) depending on the type. Also converts true/false to 1/0.
     */
    private String maybeWrapped(final MethodSpec.Builder output, final Expr expr,
                                @Nullable final GroupContext groupContext) {
        System.out.println(expr);
        String exprStr = exprToStr(output, expr, true, groupContext);

        // Some special cases to handle booleans because the or-tools API does not convert well to booleans
        if (expr instanceof MonoidLiteral && ((MonoidLiteral) expr).getValue() instanceof Boolean) {
            exprStr = (Boolean) ((MonoidLiteral) expr).getValue() ? "1" : "0";
        }
        return InferType.forExpr(expr).equals("IntVar") ? exprStr : String.format("model.newConstant(%s)", exprStr);
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
            output.addCode("\n");
            output.addComment("Table $S", table.getName());
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
                    output.addCode("\n");
                    output.addComment("Primary key constraints for $L", tableNameStr(table.getName()));

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
                    Preconditions.checkArgument(e.getFields().size() == 1);
                    final Map.Entry<IRColumn, IRColumn> next = e.getFields().entrySet().iterator().next();
                    final IRColumn child = next.getKey();
                    final IRColumn parent = next.getValue();
                    output.addCode("\n");
                    output.addComment("Foreign key constraints: $L.$L -> $L.$L",
                            child.getIRTable().getName(), child.getName(),
                            parent.getIRTable().getName(), parent.getName());
                    final String indexVarStr = "index" + intermediateViewCounter.incrementAndGet();
                    output.addStatement("final IntVar $L = model.newIntVar(0, $L - 1, \"\")",
                            indexVarStr, tableNumRowsStr(table.getName()));
                    output.beginControlFlow("for (int i = 0; i < $L; i++)",
                            tableNumRowsStr(table.getName()));
                    final String fkChild =
                            fieldNameStr(next.getKey().getIRTable().getName(), next.getKey().getName());

                    final String snippet = Joiner.on('\n').join(
                         "final long[] domain = context.getTable($S).getCurrentData()",
                         "                        .getValues($S, $L.class)",
                         "                        .stream()",
                         "                        .mapToLong(encoder::toLong).toArray()"
                    );

                    output.addStatement(snippet,
                            parent.getIRTable().getName(), parent.getName().toUpperCase(Locale.US),
                            toJavaClass(next.getValue().getType()));
                    output.addStatement("model.addLinearExpressionInDomain($L[i], $T.fromValues(domain))",
                                        fkChild, Domain.class);
                    output.endControlFlow();
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

    private String exprToStr(final MethodSpec.Builder output, final Expr expr) {
        return exprToStr(output, expr, true, null);
    }

    private String exprToStr(final MethodSpec.Builder output, final Expr expr, final boolean allowControllable,
                             @Nullable final GroupContext currentGroup) {
        final ExprToStrVisitor visitor = new ExprToStrVisitor(output, allowControllable, currentGroup);
        return Objects.requireNonNull(visitor.visit(expr, false));
    }

    private MonoidComprehension rewritePipeline(final MonoidComprehension comprehension) {
        return RewriteArity.apply(comprehension);
    }

    private <T extends Expr> String generateTupleGenericParameters(final List<T> exprs) {
        return exprs.stream().map(this::generateTupleGenericParameters)
                             .collect(Collectors.joining(", "));
    }

    private String generateTupleGenericParameters(final Expr expr) {
        return InferType.forExpr(expr);
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

    private class ExprToStrVisitor extends MonoidVisitor<String, Boolean> {
        private final MethodSpec.Builder output;
        private final boolean allowControllable;
        @Nullable private final GroupContext currentGroupContext;

        private ExprToStrVisitor(final MethodSpec.Builder output, final boolean allowControllable,
                                 @Nullable final GroupContext currentGroupContext) {
            this.output = output;
            this.allowControllable = allowControllable;
            this.currentGroupContext = currentGroupContext;
        }

        @Nullable
        @Override
        protected String visitMonoidFunction(final MonoidFunction node, @Nullable final Boolean isFunctionContext) {
            // Functions always apply on a vector. We perform a pass to identify whether we can vectorize
            // the computed inner expression within a function to avoid creating too many intermediate variables.
            final String processedArgument = visit(node.getArgument(), true);
            if (node.getFunctionName().equalsIgnoreCase("sum") ||
                node.getFunctionName().equalsIgnoreCase("count")) {
                // the assumption here for count functions it that the RewriteArity pass has already been made on
                // the input comprehensions to rewrite the column being counted with '1'.
                final String functionName = InferType.forExpr(node.getArgument()).equals("IntVar") ? "sumV" : "sum";
                return String.format("o.%s(data.stream()%n      .map(t -> %s)%n      .collect($1T.toList()))",
                        functionName, processedArgument);
            } else if (node.getFunctionName().equalsIgnoreCase("increasing")) {
                output.addStatement("o.increasing(data.stream()\n      .map(t -> $L)\n      .collect($T.toList()))",
                                    processedArgument, Collectors.class);
                return "model.newConstant(1)";
            }
            throw new UnsupportedOperationException("Unsupported aggregate function " + node.getFunctionName());
        }

        @Nullable
        @Override
        protected String visitExistsPredicate(final ExistsPredicate node, @Nullable final Boolean context) {
            final String processedArgument = visit(node.getArgument(), context);
            return String.format("o.exists(%s.stream().map(e -> e.value0()).collect($1T.toList()))",
                                 processedArgument);
        }

        @Nullable
        @Override
        protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                      @Nullable final Boolean isFunctionContext) {
            final String left = visit(node.getLeft(), isFunctionContext);
            final String right = visit(node.getRight(), isFunctionContext);
            final String op = node.getOperator();
            final String leftType = InferType.forExpr(node.getLeft());
            final String rightType = InferType.forExpr(node.getRight());

            if (leftType.equals("IntVar") || rightType.equals("IntVar")) {
                // We need to generate an IntVar.
                switch (op) {
                    case "==":
                        return String.format("o.eq(%s, %s)", left, right);
                    case "!=":
                        return String.format("o.ne(%s, %s)", left, right);
                    case "/\\":
                        return String.format("o.and(%s, %s)", left, right);
                    case "\\/":
                        return String.format("o.or(%s, %s)", left, right);
                    case "<=":
                        return String.format("o.leq(%s, %s)", left, right);
                    case "<":
                        return String.format("o.le(%s, %s)", left, right);
                    case ">=":
                        return String.format("o.geq(%s, %s)", left, right);
                    case ">":
                        return String.format("o.gt(%s, %s)", left, right);
                    case "+":
                        return String.format("o.plus(%s, %s)", left, right);
                    case "-":
                        return String.format("o.minus(%s, %s)", left, right);
                    case "*":
                        return String.format("o.mult(%s, %s)", left, right);
                    case "/":
                        return String.format("o.div(%s, %s)", left, right);
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            else {
                // Both operands are non-var, so we generate an expression in Java.
                switch (op) {
                    case "==":
                        return String.format("(%s.equals(%s))", left, right);
                    case "!=":
                        return String.format("(!%s.equals(%s))", left, right);
                    case "/\\":
                        return String.format("(%s && %s)", left, right);
                    case "\\/":
                        return String.format("(%s || %s)", left, right);
                    case "<=":
                    case "<":
                    case ">=":
                    case ">":
                    case "+":
                    case "-":
                    case "*":
                    case "/":
                        return String.format("(%s %s %s)", left, op, right);
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }

        @Nullable
        @Override
        protected String visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Boolean isFunctionContext) {
            Preconditions.checkNotNull(isFunctionContext);
            // If we are evaluating a group-by comprehension, then column accesses that happen outside the context
            // of an aggregation function must refer to the grouping column, not the inner tuple being iterated over.
            if (!isFunctionContext && currentGroupContext != null) {
                final String tableName = node.getTableName();
                final String fieldName = node.getField().getName();
                int columnNumber = 0;
                for (final ColumnIdentifier ci: currentGroupContext.qualifier.getColumnIdentifiers()) {
                    if (ci.getTableName().equalsIgnoreCase(tableName)
                            && ci.getField().getName().equalsIgnoreCase(fieldName)) {
                        return String.format("group.value%s()", columnNumber);
                    }
                    columnNumber++;
                }
                throw new UnsupportedOperationException("Could not find group-by column " + node);
            }

            if (isFunctionContext && currentGroupContext != null) {
                final String tempTableName = currentGroupContext.groupViewName.toUpperCase(Locale.US);
                final int fieldIndex = viewToFieldIndex.get(tempTableName).get(node.getField().getName());
                return String.format("t.value%s()", fieldIndex);
            }

            final String tableName = node.getField().getIRTable().getName();
            final String fieldName = node.getField().getName();
            final String iterStr = iterStr(node.getTableName());
            if (!allowControllable && fieldName.contains("CONTROLLABLE")) {
                throw new UnsupportedOperationException("Controllable variables not allowed in predicates");
            }
            return fieldNameStrWithIter(tableName, fieldName, iterStr);
        }

        @Nullable
        @Override
        protected String visitMonoidLiteral(final MonoidLiteral node, @Nullable final Boolean isFunctionContext) {
            if (node.getValue() instanceof String) {
                return node.getValue().toString().replace("'", "\"");
            } else {
                return node.getValue().toString();
            }
        }

        @Nullable
        @Override
        protected String visitMonoidComprehension(final MonoidComprehension node, @Nullable final Boolean context) {
            // We are in a subquery.
            final String newSubqueryName = SUBQUERY_NAME_PREFIX + subqueryCounter.incrementAndGet();
            addView(output, newSubqueryName, node, false, true);
            return newSubqueryName;
        }
    }

    private static class GroupContext {
        private final GroupByQualifier qualifier;
        private final String groupViewName;

        private GroupContext(final GroupByQualifier qualifier, final String groupViewName) {
            this.qualifier = qualifier;
            this.groupViewName = groupViewName;
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
    }

    private String printTime(final String event) {
        return String.format("System.out.println(\"%s: we are at \" + (System.nanoTime() - startTime))", event);
    }

    private static class HasAggregates extends MonoidVisitor<Void, Void> {
        boolean hasAggregate = false;

        @Nullable
        @Override
        protected Void visitHead(final Head node, @Nullable final Void context) {
            node.getSelectExprs().forEach(super::visit);
            return super.visitHead(node, context);
        }

        @Nullable
        @Override
        protected Void visitMonoidFunction(final MonoidFunction node, @Nullable final Void context) {
            hasAggregate = true;
            return super.visitMonoidFunction(node, context);
        }
    }

    private boolean hasAggregate(final Expr expr) {
        final HasAggregates visitor = new HasAggregates();
        visitor.visit(expr);
        return visitor.hasAggregate;
    }

    private String getTempViewName() {
        return  "tmp" + intermediateViewCounter.getAndIncrement();
    }
}