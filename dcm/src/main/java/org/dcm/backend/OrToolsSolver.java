/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.JoinPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.MonoidVisitor;
import org.dcm.compiler.monoid.TableRowGenerator;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class generates Java code that invokes the or-tools CP-SAT solver. The generated
 * class is compiled, and loaded via ReflectASM to return an IGeneratedBackend instance,
 * which can be invoked to run the solver.
 */
public class OrToolsSolver implements ISolverBackend {
    private static final Logger LOG = LoggerFactory.getLogger(OrToolsSolver.class);
    private static final String generatedBackendName = "GeneratedBackend";
    private static final MethodSpec intVarNoBounds = MethodSpec.methodBuilder("intVarNoBounds")
                                                .addModifiers(Modifier.PRIVATE)
                                                .addParameter(CpModel.class, "model", Modifier.FINAL)
                                                .addParameter(String.class, "name",  Modifier.FINAL)
                                                .returns(IntVar.class)
                                                .addStatement("return model.newIntVar(0, Integer.MAX_VALUE - 1, name)")
                                                .build();
    private static final Map<Integer, TypeSpec> tupleGen = new ConcurrentHashMap<>();
    @Nullable private IGeneratedBackend generatedBackend = null;
    @Nullable private IRContext context = null;

    @Override
    public Map<IRTable, Result<? extends Record>> runSolver(final DSLContext dbCtx,
                                                            final Map<String, IRTable> irTables) {
        Preconditions.checkNotNull(generatedBackend);
        Preconditions.checkNotNull(context);
        final long now = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            generatedBackend.solve(context);
        }
        LOG.info("Completed in {}", (System.currentTimeMillis() - now));
        return null;
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
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("solve");
        addInitializer(builder);
        addArrayDeclarations(builder, context, intVarNoBounds);
        nonConstraintViews.forEach((name, comprehension) ->
                addNonConstraintView(builder, name, comprehension, context));
        final MethodSpec solveMethod = builder.addStatement("return null").build();

        final TypeSpec.Builder backendClassBuilder = TypeSpec.classBuilder(generatedBackendName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(IGeneratedBackend.class)
                .addMethod(solveMethod)
                .addMethod(intVarNoBounds);
        tupleGen.values().forEach(backendClassBuilder::addType); // Add tuple types
        final TypeSpec spec = backendClassBuilder.build();
        return compile(spec);
    }


    private void addNonConstraintView(final MethodSpec.Builder builder, final String viewName,
                                      final MonoidComprehension comprehension, final IRContext context) {
        Preconditions.checkNotNull(comprehension.getHead());
        builder.addCode("\n").addComment("Non-constraint view $L", viewName);

        final List<BinaryOperatorPredicate> wherePredicates = new ArrayList<>();
        final List<JoinPredicate> joinPredicates = new ArrayList<>();
        final List<BinaryOperatorPredicateWithAggregate> aggregatePredicates = new ArrayList<>();
        final List<TableRowGenerator> tableRowGenerators = new ArrayList<>();
        comprehension.getQualifiers().forEach(
            q -> {
                if (q instanceof BinaryOperatorPredicateWithAggregate) {
                    aggregatePredicates.add((BinaryOperatorPredicateWithAggregate) q);
                } else if (q instanceof JoinPredicate) {
                    joinPredicates.add((JoinPredicate) q);
                } else if (q instanceof BinaryOperatorPredicate) {
                    wherePredicates.add((BinaryOperatorPredicate) q);
                } else if (q instanceof TableRowGenerator) {
                    tableRowGenerators.add((TableRowGenerator) q);
                } else {
                    throw new IllegalArgumentException();
                }
            }
        );
        Preconditions.checkArgument(aggregatePredicates.size() == 0); // Temporary

        // Create nested for loops
        final int tupleSize = comprehension.getHead().getSelectExprs().size();
        final TypeSpec tupleSpec = tupleGen.computeIfAbsent(tupleSize, OrToolsSolver::tupleGen);

        // Initialize an arraylist to collect results
        final String viewRecords = nonConstraintViewName(viewName);
        builder.addStatement("final $T<$N> $L = new $T<>()", List.class, tupleSpec, viewRecords, ArrayList.class);
        tableRowGenerators.forEach(tr -> {
            final String tableName = tr.getTable().getName();
            final String tableNumRowsStr = tableNumRowsStr(tableName);
            final String iterStr = iterStr(tr.getTable().getAliasedName());
            builder.beginControlFlow("for (int $1L = 0; $1L < $2L; $1L++)", iterStr, tableNumRowsStr);
        });

        final String joinPredicateStr = joinPredicates.stream()
                                                    .map(OrToolsSolver::exprToStr)
                                                    .collect(Collectors.joining(" \n    && "));
        final String wherePredicateStr = wherePredicates.stream()
                                                .map(OrToolsSolver::exprToStr)
                                                .collect(Collectors.joining(" \n    && "));
        final String predicateStr = Stream.of(joinPredicateStr, wherePredicateStr)
                                       .filter(s -> !s.equals(""))
                                       .collect(Collectors.joining(" \n    && "));
        // Add filter predicate
        builder.beginControlFlow("if ($L)", predicateStr);

        // If predicate is true, then add select expressions to result set
        final String headItemsStr = comprehension.getHead().getSelectExprs().stream()
                                              .map(expr -> {
                                                    if (expr instanceof MonoidFunction) {
                                                        throw new UnsupportedOperationException();
                                                    }
                                                    return exprToStr(expr);
                                                })
                                              .collect(Collectors.joining(",\n    "));
        // Add tuples to result set
        builder.addStatement("$L.add(new Tuple$L(\n    $L\n    ))", viewRecords, tupleSize, headItemsStr);
        builder.endControlFlow(); // end filter predicate if statement

        // Pop nested for loops
        tableRowGenerators.forEach(tr -> builder.endControlFlow());

        // Add numRows for view
        builder.addStatement("final int $L = $L.size()", tableNumRowsStr(viewName), nonConstraintViewName(viewName));
    }

    private void addArrayDeclarations(final MethodSpec.Builder builder, final IRContext context,
                                      final MethodSpec intVarNoBounds) {
        // Tables
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            builder.addCode("\n");
            builder.addComment("Table $S", table.getName());
            builder.addStatement("final int $L = context.getTable($S).getNumRows()",
                                 tableNumRowsStr(table.getName()), table.getName());

            builder.addStatement("final $T<? extends $T> $L = context.getTable($S).getCurrentData()",
                                 List.class, Record.class, tableNameStr(table.getName()), table.getName());
            // Fields
            for (final Map.Entry<String, IRColumn> fieldEntrySet : table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();
                if (field.isControllable()) {
                    final String variableName = fieldNameStr(table.getName(), fieldName);
                    builder.addStatement("final $T[] $L = new $T[$L]", IntVar.class, variableName, IntVar.class,
                                                                       table.getNumRows())
                            .beginControlFlow("for (int i = 0; i < $L; i++)", table.getNumRows())
                            .addStatement("$L[i] = $N(model, $S)", variableName, intVarNoBounds, fieldName)
                            .endControlFlow();
                }
            }
        }
    }

    private void addInitializer(final MethodSpec.Builder builder) {
        builder.addModifiers(Modifier.PUBLIC)
               .returns(Map.class)
               .addParameter(IRContext.class, "context")
               .addComment("Create the model.")
               .addStatement("final $T model = new $T()", CpModel.class, CpModel.class)
               .addCode("\n");
    }

    private static String tableNameStr(final String tableName) {
        return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName);
    }

    private static String fieldNameStr(final String tableName, final String fieldName) {
        return String.format("%s%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName),
                                     CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName));
    }

    private static String fieldNameStrWithIter(final String tableName, final String fieldName) {
        return String.format("%s.get(%s).get(\"%s\")", tableNameStr(tableName), iterStr(tableName), fieldName);
    }

    private static String tableNumRowsStr(final String tableName) {
        return String.format("%sNumRows", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private static String iterStr(final String tableName) {
        return String.format("%sIter", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private static String nonConstraintViewName(final String tableName) {
        return String.format("%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private List<String> compile(final TypeSpec spec) {
        final JavaFile javaFile = JavaFile.builder("org.dcm.backend", spec).build();
        LOG.info("Generating Java or-tools code: {}\n", javaFile.toString());

        // Compile Java code. This steps requires an SDK, and a JRE will not suffice
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, null);
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
            File classesDir = new File("/tmp/");
            // Load and instantiate compiled class.
            URLClassLoader classLoader;
            // Loading the class
            classLoader = URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()});
            Class<?> cls;
            cls = Class.forName(String.format("org.dcm.backend.%s", generatedBackendName), true, classLoader);
            ConstructorAccess<?> access = ConstructorAccess.get(cls);
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
        return null;
    }

    /**
     * Create a tuple type with 'numFields' entries. Results in a generic "plain old java object"
     * with a getter per field.
     */
    private static TypeSpec tupleGen(final int numFields) {
        final TypeSpec.Builder classBuilder = TypeSpec.classBuilder("Tuple" + numFields)
                                                     .addModifiers(Modifier.FINAL, Modifier.PRIVATE);
        final MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                                                         .addModifiers(Modifier.PRIVATE);
        for (int i = 0; i < numFields; i++) {
            final TypeVariableName type = TypeVariableName.get("T" + i);
            // Add parameter to constructor
            constructor.addParameter(type, "t" + i, Modifier.FINAL)
                       .addStatement("this.$1L = $1L", "t" + i); // assign parameters to fields

            // Create getter
            final MethodSpec getter = MethodSpec.methodBuilder("value" + i)
                    .returns(type)
                    .addStatement("return t" + i)
                    .build();

            // Add field and getter to class
            classBuilder.addTypeVariable(type)
                        .addField(type, "t" + i, Modifier.PRIVATE, Modifier.FINAL)
                        .addMethod(getter);
        }

        return classBuilder.addMethod(constructor.build())
                           .build();
    }

    private static String exprToStr(final Expr expr) {
        final ExprToStrVisitor visitor = new ExprToStrVisitor();
        return Objects.requireNonNull(visitor.visit(expr));
    }

    private static class ExprToStrVisitor extends MonoidVisitor<String, Void> {
        @Nullable
        @Override
        protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                      @Nullable final Void context) {
            final String left = visit(node.getLeft());
            final String right = visit(node.getRight());
            final String op = node.getOperator();
            switch (op) {
                case "==":
                    return String.format("(%s.equals(%s))", left, right);
                case "!=":
                    return String.format("(!%s.equals(%s))", left, right);
                case "/\\":
                    return String.format("(%s \n    && %s))", left, right);
                case "\\/":
                    return String.format("(%s \n    || %s))", left, right);
                case "<=":
                case "<":
                case ">=":
                case ">":
                case "+":
                case "-":
                case "*":
                case "/":
                    return String.format("(%s \n    %s %s))", left, op, right);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Nullable
        @Override
        protected String visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Void context) {
            final String tableName = node.getTableName();
            final String fieldName = node.getField().getName();
            return fieldNameStrWithIter(tableName, fieldName);
        }

        @Nullable
        @Override
        protected String visitMonoidLiteral(final MonoidLiteral node, @Nullable final Void context) {
            if (node.getValue() instanceof String) {
                return node.getValue().toString().replace("'", "\"");
            } else {
                return node.getValue().toString();
            }
        }
    }
}
