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
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.compiler.monoid.MonoidComprehension;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrToolsSolver implements ISolverBackend {
    private static final Logger LOG = LoggerFactory.getLogger(OrToolsSolver.class);
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

    @Override
    public List<String> generateModelCode(final IRContext context,
                                          final Map<String, MonoidComprehension> nonConstraintViews,
                                          final Map<String, MonoidComprehension> constraintViews,
                                          final Map<String, MonoidComprehension> objectiveFunctions) {
        final MethodSpec intVarNoBounds = MethodSpec.methodBuilder("intVarNoBounds")
                .addModifiers(Modifier.PRIVATE)
                .addParameter(CpModel.class, "model")
                .addParameter(String.class, "name")
                .returns(IntVar.class)
                .addStatement("return model.newIntVar(0, Integer.MAX_VALUE - 1, name)")
                .build();

        MethodSpec.Builder builder = MethodSpec.methodBuilder("solve");
        addInitializer(builder);
        addArrayDeclarations(builder, context, intVarNoBounds);
        final MethodSpec solveMethod = builder.addStatement("return null").build();

        final TypeSpec spec = TypeSpec.classBuilder("GeneratedBackend")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(IGeneratedBackend.class)
                .addMethod(solveMethod)
                .addMethod(intVarNoBounds)
                .build();
        return compile(spec);
    }

    private MethodSpec.Builder addArrayDeclarations(final MethodSpec.Builder builder,
                                                    final IRContext context,
                                                    final MethodSpec intVarNoBounds) {

        final Map<IRTable, Map<String, IntVar[]>> tableToField = new HashMap<>();
        // Tables
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }

            builder.addComment("Table $S", table.getName());
            builder.addStatement("final int $L = context.getTable($S).getNumRows()",
                                 tableNumRowsStr(table.getName()), table.getName());
            // Fields
            for (final Map.Entry<String, IRColumn> fieldEntrySet : table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();
                if (field.isControllable()) {
                    final String variableName = variableNameStr(table.getName(), fieldName);
                    builder.addStatement("final $T[] $L = new $T[$L]", IntVar.class, variableName, IntVar.class,
                                                                       table.getNumRows())
                            .beginControlFlow("for (int i = 0; i < $L; i++)", table.getNumRows())
                            .addStatement("$L[i] = $N(model, $S)", variableName, intVarNoBounds, fieldName)
                            .endControlFlow();
                }
            }
        }
        return builder;
    }

    private MethodSpec.Builder addInitializer(final MethodSpec.Builder builder) {
        return builder
                .addModifiers(Modifier.PUBLIC)
                .returns(Map.class)
                .addParameter(IRContext.class, "context")
                .addComment("Create the model.")
                .addStatement("final $T model = new $T()", CpModel.class, CpModel.class)
                .addCode("\n");
    }

    private String variableNameStr(final String tableName, final String fieldName) {
        return String.format("%s%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName),
                                     CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName));
    }


    private String tableNumRowsStr(final String tableName) {
        return String.format("%sNumRows", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }


    private List<String> compile(final TypeSpec spec) {
        final JavaFile javaFile = JavaFile.builder("com.vrg.backend", spec)
                                          .build();
        LOG.info("Generating Java or-tools code: {}\n", javaFile.toString());

        // Compile Java code. This steps requires an SDK, and a JRE will not suffice
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnit =
                Collections.singleton(javaFile.toJavaFileObject());

        try {
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
            cls = Class.forName("com.vrg.backend.GeneratedBackend", true, classLoader);
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
}
