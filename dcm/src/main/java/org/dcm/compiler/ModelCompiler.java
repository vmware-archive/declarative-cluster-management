/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.Model;
import org.dcm.backend.ISolverBackend;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     */
    @CanIgnoreReturnValue
    public List<String> compile(final List<CreateView> views, final ISolverBackend backend) {
        LOG.debug("Compiling the following views\n{}", views);
        // First, we extract all the necessary views from the input code
        final ReferencedSymbols symbols = new ReferencedSymbols();

        views.forEach(view -> extractSymbols(view, symbols));
        final Map<String, MonoidComprehension> nonConstraintForAlls =
                parseNonConstraintViews(symbols.getNonConstraintViews());
        final Map<String, MonoidComprehension> constraintForAlls = parseViews(symbols.getConstraintViews());
        final Map<String, MonoidComprehension> objFunctionForAlls = parseViews(symbols.getObjectiveFunctionViews());
        //
        // Code generation begins here.
        //
        return backend.generateModelCode(irContext, nonConstraintForAlls, constraintForAlls, objFunctionForAlls);
    }


    @CanIgnoreReturnValue
    public List<String> updateData(final IRContext context, final ISolverBackend backend) {
        return backend.generateDataCode(context);
    }


    private void extractSymbols(final CreateView view, final ReferencedSymbols symbols) {
            final SymbolExtractingVisitor visitor = new SymbolExtractingVisitor();
            // updates class field with all the existing views symbols
            visitor.process(view, symbols);
    }

    /**
     * Stage 2: converts a view into a ForAllStatement.
     * @param views a map of String (name) -> View pairs.
     * @return map of String (name) -> ForAllStatement pairs corresponding to the views parameter
     */
    private Map<String, MonoidComprehension> parseNonConstraintViews(final Map<String, Query> views) {
        return views.entrySet()
                    .stream()
                    .peek(es -> createIRTablesForNonConstraintViews(es.getKey(), es.getValue()))
                    .map(es -> Map.entry(es.getKey(), TranslateViewToIR.apply(es.getValue(), irContext)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * A pass to create IRTable entries for non-constraint views. These views are used as intermediate
     * computations, so we need an IRTable entry to track relevant metadata (like column type information).
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
    private void createIRTablesFromSelectItems(final List<SelectItem> selectItems, final Set<IRTable> tables,
                                               final String viewName) {
        final IRTable viewTable = new IRTable(null, viewName, viewName);
        for (final SelectItem selectItem: selectItems) {
            if (selectItem instanceof SingleColumn) {
                final SingleColumn singleColumn = (SingleColumn) selectItem;
                final Expression expression = singleColumn.getExpression();
                if (singleColumn.getAlias().isPresent()) {
                    final IRColumn.FieldType fieldType;
                    if (expression instanceof Identifier) {
                        fieldType = irContext.getColumnIfUnique(expression.toString(), tables).getType();
                    } else if (expression instanceof DereferenceExpression) {
                        fieldType = TranslateViewToIR
                                   .getIRColumnFromDereferencedExpression((DereferenceExpression) expression, irContext)
                                   .getType();
                    } else {
                        LOG.warn("Guessing FieldType for column {} in non-constraint view {} to be INT",
                                singleColumn.getAlias(), viewName);
                        fieldType = IRColumn.FieldType.INT;
                    }
                    final IRColumn column = new IRColumn(viewTable, null, fieldType,
                            singleColumn.getAlias().get().toString());
                    viewTable.addField(column);
                } else if (expression instanceof Identifier) {
                    final IRColumn columnIfUnique = irContext.getColumnIfUnique(expression.toString(), tables);
                    final IRColumn newColumn = new IRColumn(viewTable, null, columnIfUnique.getType(),
                            columnIfUnique.getName());
                    viewTable.addField(newColumn);
                } else if (expression instanceof DereferenceExpression) {
                    final DereferenceExpression derefExpression = (DereferenceExpression) expression;
                    final IRColumn irColumn =
                            TranslateViewToIR.getIRColumnFromDereferencedExpression(derefExpression, irContext);
                    final IRColumn newColumn = new IRColumn(viewTable, null, irColumn.getType(),
                            irColumn.getName());
                    viewTable.addField(newColumn);
                } else {
                    throw new RuntimeException("SelectItem type is not a column but does not have an alias");
                }
            } else if (selectItem instanceof AllColumns) {
                tables.forEach(
                        table -> table.getIRColumns().forEach((fieldName, irColumn) -> {
                            viewTable.addField(irColumn);
                        })
                );
            }
        }
        irContext.addAliasedOrViewTable(viewTable);
    }

    /**
     * Stage 2: converts a view into a ForAllStatement.
     * @param views a map of String (name) -> View pairs.
     * @return map of String (name) -> ForAllStatement pairs corresponding to the views parameter
     */
    private Map<String, MonoidComprehension> parseViews(final Map<String, Query> views) {
        final Map<String, MonoidComprehension> result = new HashMap<>();
        views.forEach((key, value) -> result.put(key, TranslateViewToIR.apply(value, irContext)));
        return result;
    }
}