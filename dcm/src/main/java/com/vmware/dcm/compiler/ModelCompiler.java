/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vmware.dcm.IRContext;
import com.vmware.dcm.Model;
import com.vmware.dcm.ViewsWithChecks;
import com.vmware.dcm.backend.ISolverBackend;
import com.vmware.dcm.IRColumn;
import com.vmware.dcm.IRTable;
import com.vmware.dcm.compiler.monoid.MonoidComprehension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ModelCompiler {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);
    private final IRContext irContext;

    public ModelCompiler(final IRContext irContext) {
        this.irContext = irContext;
    }

    /**
     * Entry point to compile views into list comprehensions
     * @param views a list of strings, each of which is a view statement
     * @param backend an ISolverBackend instance.
     * @return A list of strings representing the program that was compiled
     */
    @CanIgnoreReturnValue
    public List<String> compile(final List<ViewsWithChecks> views, final ISolverBackend backend) {
        LOG.debug("Compiling the following views\n{}", views);
        // First, we extract all the necessary views from the input code
        final ReferencedSymbols symbols = new ReferencedSymbols();
        splitByType(views, symbols);
        final Map<String, MonoidComprehension> nonConstraintForAlls =
                parseNonConstraintViews(symbols.getNonConstraintViews());
        final Map<String, MonoidComprehension> constraintForAlls = parseViews(symbols.getConstraintViews());
        final Map<String, MonoidComprehension> objFunctionForAlls = parseViews(symbols.getObjectiveFunctionViews());
        //
        // Code generation begins here.
        //
        return backend.generateModelCode(irContext, nonConstraintForAlls, constraintForAlls, objFunctionForAlls);
    }

    private void splitByType(final List<ViewsWithChecks> viewsWithChecks, final ReferencedSymbols symbols) {
        viewsWithChecks.forEach(view -> {
                final CreateView createView = view.getCreateView();
                final String viewName = createView.getName().toString();
                if (view.getCheckExpression().isPresent()) {
                    symbols.getConstraintViews().put(viewName, view);
                } else if (viewName.toLowerCase(Locale.US).startsWith("objective_")) {
                    symbols.getObjectiveFunctionViews().put(viewName, view);
                } else {
                    symbols.getNonConstraintViews().put(viewName, view);
                }
            }
        );
    }

    @CanIgnoreReturnValue
    public List<String> updateData(final IRContext context, final ISolverBackend backend) {
        return backend.generateDataCode(context);
    }

    /**
     * Stage 2: converts a view into a ForAllStatement.
     * @param views a map of String (name) -> View pairs.
     * @return map of String (name) -> ForAllStatement pairs corresponding to the views parameter
     */
    private Map<String, MonoidComprehension> parseNonConstraintViews(final Map<String, ViewsWithChecks> views) {
        return views.entrySet()
                    .stream()
                    .map(es -> Map.entry(es.getKey(), es.getValue().getCreateView().getQuery()))
                    .peek(es -> createIRTablesForNonConstraintViews(es.getKey(), es.getValue()))
                    .map(es -> Map.entry(es.getKey(), TranslateViewToIR.apply(es.getValue(), Optional.empty(),
                                                                              irContext)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
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

    /**
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

    /**
     * Stage 2: converts a view into a ForAllStatement.
     * @param views a map of String (name) -> View pairs.
     * @return map of String (name) -> ForAllStatement pairs corresponding to the views parameter
     */
    private Map<String, MonoidComprehension> parseViews(final Map<String, ViewsWithChecks> views) {
        final Map<String, MonoidComprehension> result = new HashMap<>();
        views.forEach((key, value) -> result.put(key, TranslateViewToIR.apply(value.getCreateView().getQuery(),
                                                                              value.getCheckExpression(),
                                                                              irContext)));
        return result;
    }

    /**
     * Identifies and adds IRColumns to a given IRTable (viewTable) by scanning the select items in a particular view.
     */
    private static class IRColumnsFromSelectItems extends DefaultTraversalVisitor<Void, Optional<String>> {
        private final IRContext irContext;
        private final IRTable viewTable;
        private final Set<IRTable> tablesReferencedInView;

        private IRColumnsFromSelectItems(final IRContext irContext, final IRTable viewTable,
                                         final Set<IRTable> tablesReferencedInView) {
            this.irContext = irContext;
            this.viewTable = viewTable;
            this.tablesReferencedInView = tablesReferencedInView;
        }

        @Override
        protected Void visitSingleColumn(final SingleColumn node, final Optional<String> context) {
            final int before = viewTable.getIRColumns().size();
            super.visitSingleColumn(node, node.getAlias().map(Identifier::getValue));
            if (viewTable.getIRColumns().size() == before) {
                // Was neither an identifier nor a dereference expression.
                // We therefore assume its a supported expression, but require
                // that it have an alias
                LOG.warn("Guessing FieldType for column {} in non-constraint view {} to be INT",
                         node.getAlias(), viewTable.getName());
                final String alias = node.getAlias().orElseThrow().getValue();
                final IRColumn.FieldType intType = IRColumn.FieldType.INT;
                final IRColumn newColumn = new IRColumn(viewTable, null, intType, alias);
                viewTable.addField(newColumn);
            }
            return null;
        }

        @Override
        protected Void visitAllColumns(final AllColumns node, final Optional<String> context) {
            tablesReferencedInView.forEach(
                table -> table.getIRColumns().forEach((fieldName, irColumn) -> viewTable.addField(irColumn))
            );
            return null;
        }

        @Override
        protected Void visitIdentifier(final Identifier node, final Optional<String> context) {
            final IRColumn columnIfUnique = irContext.getColumnIfUnique(node.toString(), tablesReferencedInView);
            final IRColumn newColumn = new IRColumn(viewTable, null, columnIfUnique.getType(),
                                                    context.orElse(columnIfUnique.getName()));
            viewTable.addField(newColumn);
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(final DereferenceExpression node, final Optional<String> context) {
            final IRColumn irColumn = TranslateViewToIR.getIRColumnFromDereferencedExpression(node, irContext);
            final IRColumn newColumn = new IRColumn(viewTable, null, irColumn.getType(),
                                                    context.orElse(irColumn.getName()));
            viewTable.addField(newColumn);
            return null;
        }
    }
}