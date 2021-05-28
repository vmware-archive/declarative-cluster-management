/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vmware.dcm.IRContext;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.Model;
import com.vmware.dcm.ViewsWithAnnotations;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.compiler.ir.ListComprehension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class ModelCompiler {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);
    private final IRContext irContext;

    public ModelCompiler(final IRContext irContext) {
        this.irContext = irContext;
    }

    /**
     * Entry point to compile views into backend-specific code
     * @param views a list of strings, each of which is a view statement
     * @param backend an ISolverBackend instance.
     * @return A list of strings representing the output program that was compiled
     */
    @CanIgnoreReturnValue
    public List<String> compile(final List<ViewsWithAnnotations> views, final ISolverBackend backend) {
        LOG.debug("Compiling the following views\n{}", views);
        // First, we extract all the necessary views from the input code
        final Program<ViewsWithAnnotations> sqlProgram = toSqlProgram(views);

        // Create IRTable entries for non-constraint views
        sqlProgram.forEachNonConstraint((name, view) ->
                                         createIRTablesForNonConstraintViews(name, view.getCreateView().getQuery()));

        // Convert from SQL to list comprehension syntax
        final Program<ListComprehension> irProgram = sqlProgram.transformWith(this::toListComprehension);

        // Backend-specific code generation begins here.
        return backend.generateModelCode(irContext, irProgram);
    }

    private Program<ViewsWithAnnotations> toSqlProgram(final List<ViewsWithAnnotations> viewsWithChecks) {
        final Program<ViewsWithAnnotations> program = new Program<>();
        viewsWithChecks.forEach(view -> {
                final CreateView createView = view.getCreateView();
                final String viewName = createView.getName().toString();
                if (view.getCheckExpression().isPresent()) {
                    program.constraintViews().put(viewName, view);
                } else if (view.isObjective()) {
                    program.objectiveFunctionViews().put(viewName, view);
                } else {
                    program.nonConstraintViews().put(viewName, view);
                }
            }
        );
        return program;
    }

    /*
     * A pass to create IRTable entries for non-constraint views. These views are used as intermediate
     * computations, so it is convenient in later stages of the compiler to have an IRTable entry for such views
     * to track relevant metadata (like column type information).
     *
     * @param viewName view being parsed
     * @param view AST representing the view
     */
    private void createIRTablesForNonConstraintViews(final String viewName, final Query view) {
        final FromExtractor fromParser = new FromExtractor(irContext);
        fromParser.process(view.getQueryBody());

        final Set<IRTable> tables = fromParser.getTables();
        final List<SelectItem> selectItems = ((QuerySpecification) view.getQueryBody()).getSelect().getSelectItems();
        createIRTablesFromSelectItems(selectItems, tables, viewName);
    }

    /*
     * Given a list of select items, constructs IRColumns and IRTable entries for them.
     */
    private void createIRTablesFromSelectItems(final List<SelectItem> selectItems,
                                               final Set<IRTable> tablesReferencedInView, final String viewName) {
        final IRTable viewTable = new IRTable(null, viewName, viewName);
        selectItems.forEach(selectItem -> {
            final IRColumnsFromSelectItems visitor = new IRColumnsFromSelectItems(irContext, viewTable,
                                                                                  tablesReferencedInView);
            visitor.process(selectItem); // updates viewTable with new columns
        });
        irContext.addAliasedOrViewTable(viewTable);
    }

    private ListComprehension toListComprehension(final String name, final ViewsWithAnnotations view) {
        return TranslateViewToIR.apply(view.getCreateView().getQuery(), view.getCheckExpression(), irContext);
    }
}