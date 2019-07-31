/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
import org.dcm.Model;
import org.dcm.backend.ISolverBackend;
import org.dcm.compiler.monoid.BinaryOperatorPredicate;
import org.dcm.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import org.dcm.compiler.monoid.ColumnIdentifier;
import org.dcm.compiler.monoid.Expr;
import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.GroupByQualifier;
import org.dcm.compiler.monoid.Head;
import org.dcm.compiler.monoid.JoinPredicate;
import org.dcm.compiler.monoid.MonoidComprehension;
import org.dcm.compiler.monoid.MonoidFunction;
import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.Qualifier;
import org.dcm.compiler.monoid.TableRowGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ModelCompiler {
    private static final Logger LOG = LoggerFactory.getLogger(Model.class);
    private static final Map<ArithmeticBinaryExpression.Operator, String> ARITHMETIC_OP_TABLE =
            new EnumMap<>(ArithmeticBinaryExpression.Operator.class);
    private static final Map<ComparisonExpression.Operator, String> COMPARISON_OP_TABLE =
            new EnumMap<>(ComparisonExpression.Operator.class);
    private static final Map<LogicalBinaryExpression.Operator, String> LOGICAL_OP_TABLE =
            new EnumMap<>(LogicalBinaryExpression.Operator.class);

    static {
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.ADD, "+");
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.SUBTRACT, "-");
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.MULTIPLY, "*");
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.DIVIDE, "/");
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.MODULUS, "mod");

        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.EQUAL, "==");
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.LESS_THAN, "<");
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.GREATER_THAN, ">");
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, "<=");
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, ">=");
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.NOT_EQUAL, "!=");

        LOGICAL_OP_TABLE.put(LogicalBinaryExpression.Operator.AND, "/\\");
        LOGICAL_OP_TABLE.put(LogicalBinaryExpression.Operator.OR, "\\/");
    }

    private final IRContext irContext;

    public ModelCompiler(final IRContext irContext) {
        this.irContext = irContext;
    }

    /**
     * Entry point to compile views into Minizinc
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
        final Map<String, MonoidComprehension> result = new HashMap<>();
        views.forEach((key, value) -> result.put(key, parseViewMonoid(key, value, true)));
        return result;
    }

    /**
     * Stage 2: converts a view into a ForAllStatement.
     * @param views a map of String (name) -> View pairs.
     * @return map of String (name) -> ForAllStatement pairs corresponding to the views parameter
     */
    private Map<String, MonoidComprehension> parseViews(final Map<String, Query> views) {
        final Map<String, MonoidComprehension> result = new HashMap<>();
        views.forEach((key, value) -> result.put(key, parseViewMonoid(key, value, false)));
        return result;
    }


    /**
     * Converts a view into a ForAllStatement.
     * @param view a parsed View statement
     * @return A ForAllStatement corresponding to the view parameter
     */
    private MonoidComprehension parseViewMonoid(final String viewName, final Query view,
                                                final boolean createIrTableForView) {
        final FromExtractor fromParser = new FromExtractor(irContext);
        fromParser.process(view.getQueryBody());

        final Set<IRTable> tables = fromParser.getTables();
        final Optional<Expression> where = ((QuerySpecification) view.getQueryBody()).getWhere();
        final List<Expression> joinConditions = fromParser.getJoinConditions();
        final Optional<Expression> having = ((QuerySpecification) view.getQueryBody()).getHaving();
        final Optional<GroupBy> groupBy = ((QuerySpecification) view.getQueryBody()).getGroupBy();

        // Construct Monoid Comprehension
        final List<SelectItem> selectItems = ((QuerySpecification) view.getQueryBody()).getSelect().getSelectItems();
        final List<Expr> selectItemExpr = processSelectItems(selectItems, tables, createIrTableForView, viewName);
        final Head head = new Head(selectItemExpr);

        final List<Qualifier> qualifiers = new ArrayList<>();
        tables.forEach(t -> qualifiers.add(new TableRowGenerator(t)));
        where.ifPresent(e -> qualifiers.addAll(processWhereExpression(e, tables, false)));
        having.ifPresent(e -> qualifiers.addAll(processWhereExpression(e, tables, true)));

        joinConditions.forEach(e -> {
            final List<Qualifier> joinQualifiers = processWhereExpression(e, tables, false);
            for (final Qualifier qualifier: joinQualifiers) {
                assert qualifier instanceof BinaryOperatorPredicate;
                qualifiers.add(new JoinPredicate((BinaryOperatorPredicate) qualifier));
            }
        });

        if (groupBy.isPresent()) {
            final List<GroupingElement> groupingElement = groupBy.get().getGroupingElements();
            final List<ColumnIdentifier> columnIdentifiers = processSimpleGroupBy(groupingElement, tables);
            final GroupByQualifier groupByQualifier = new GroupByQualifier(columnIdentifiers);
            final MonoidComprehension comprehension = new MonoidComprehension(head, qualifiers);
            return new GroupByComprehension(comprehension, groupByQualifier);
        }
        return new MonoidComprehension(head, qualifiers);
    }

    private List<Expr> processSelectItems(final List<SelectItem> selectItems, final Set<IRTable> tables,
                                          final boolean createIrTableForView, final String viewName) {
        final List<Expr> exprs = new ArrayList<>();

        // This represents an IRTable that we create for non-constraint views. It should not be created
        // for constraint or objective function views.
        final IRTable viewTable = createIrTableForView ? new IRTable(null, viewName, viewName) : null;
        for (final SelectItem selectItem: selectItems) {
            if (selectItem instanceof SingleColumn) {
                final SingleColumn singleColumn = (SingleColumn) selectItem;
                final Expression expression = singleColumn.getExpression();
                if (createIrTableForView) {
                    if (singleColumn.getAlias().isPresent()) {
                        final IRColumn column = new IRColumn(viewTable, null, singleColumn.getAlias()
                                .get().toString());
                        viewTable.addField(column);
                    } else if (expression instanceof Identifier) {
                        final IRColumn columnIfUnique = irContext.getColumnIfUnique(expression.toString(), tables);
                        final IRColumn newColumn = new IRColumn(viewTable, null, columnIfUnique.getName());
                        viewTable.addField(newColumn);
                    } else if (expression instanceof DereferenceExpression) {
                        final DereferenceExpression derefExpression = (DereferenceExpression) expression;
                        final IRColumn irColumn = getIRColumnFromDereferencedExpression(derefExpression);
                        final IRColumn newColumn = new IRColumn(viewTable, null, irColumn.getName());
                        viewTable.addField(newColumn);
                    } else {
                        throw new RuntimeException("SelectItem type is not a column but does not have an alias");
                    }
                }
                final List<Expr> result = processArithmeticExpression(expression, tables, false);
                assert result.size() == 1;
                final Expr expr = result.get(0);
                singleColumn.getAlias().ifPresent(v -> expr.setAlias(v.toString()));
                exprs.add(expr);

            } else if (selectItem instanceof AllColumns) {
                tables.forEach(
                    table -> table.getIRColumns().forEach((fieldName, irColumn) -> {
                        if (createIrTableForView) {
                            viewTable.addField(irColumn);
                        }
                        exprs.add(new ColumnIdentifier(table.getName(), irColumn, false));
                    })
                );
            }
        }

        if (createIrTableForView) {
            irContext.addAliasedOrViewTable(viewTable);
        }

        return exprs;
    }

    private List<ColumnIdentifier> processSimpleGroupBy(final List<GroupingElement> groupingElements,
                                                        final Set<IRTable> tables) {
        final List<ColumnIdentifier> columnIdentifiers = new ArrayList<>();

        for (final GroupingElement element: groupingElements) {
            assert element instanceof SimpleGroupBy;
            final List<Expression> columnExpressions = ((SimpleGroupBy) element).getColumnExpressions();
            assert !columnExpressions.isEmpty();

            for (final Expression columnExpression : columnExpressions) {
                final ColumnIdentifier columnIdentifierFromField =
                        getColumnIdentifierFromField(columnExpression, tables);
                columnIdentifiers.add(columnIdentifierFromField);
            }
        }
        return columnIdentifiers;
    }

    /**
     * Converts a WHERE expression stack obtained by a WhereExpressionParser to a Minizinc expression
     */
    private ImmutableList<Qualifier> processWhereExpression(final Expression expression,
                                                            final Set<IRTable> tablesReferencedInView,
                                                            final boolean isAggregate) {
        final ExpressionTraverser traverser = new ExpressionTraverser();
        traverser.process(expression);
        final ArrayDeque<Node> stack = traverser.getExpressionStack();
        if (stack.size() == 0) {
            return ImmutableList.of();
        }
        final ArrayDeque<Expr> operands = convertToExpr(expression, tablesReferencedInView, isAggregate);
        final List<Qualifier> predicates = new ArrayList<>();
        for (final Expr expr: operands) {
            assert expr instanceof BinaryOperatorPredicate : expr;
            predicates.add((BinaryOperatorPredicate) expr);
        }
        assert predicates.size() > 0;
        return ImmutableList.copyOf(predicates);
    }

    /**
     * Process arithmetic expression
     */
    private List<Expr> processArithmeticExpression(final Expression expression,
                                                   final Set<IRTable> tablesReferencedInView,
                                                   final boolean isAggregate) {
        final ExpressionTraverser traverser = new ExpressionTraverser();
        traverser.process(expression);
        final ArrayDeque<Node> stack = traverser.getExpressionStack();
        if (stack.size() == 0) {
            return ImmutableList.of();
        }
        final ArrayDeque<Expr> operands = convertToExpr(expression, tablesReferencedInView, isAggregate);
        return ImmutableList.copyOf(operands);
    }

    private ArrayDeque<Expr> convertToExpr(final Expression expression, final Set<IRTable> tablesReferencedInView,
                                           final boolean isAggregate) {
        final ExpressionTraverser traverser = new ExpressionTraverser();
        traverser.process(expression);
        final ArrayDeque<Node> stack = traverser.getExpressionStack();
        final ArrayDeque<Expr> operands = new ArrayDeque<>();
        while (stack.size() > 0) {
            final Node node = stack.pop();
            if (node instanceof SubqueryExpression) {
                final Query subQuery = ((SubqueryExpression) node).getQuery();
                operands.push(parseViewMonoid("", subQuery, false));
            } else if (node instanceof FunctionCall) {
                // Only having clauses will have function calls in the expression.
                final FunctionCall functionCall = (FunctionCall) node;
                final Expr function;
                final String mnzFunctionName = functionCall.getName().toString().toLowerCase(Locale.US);
                if (functionCall.getArguments().size() == 1) {
                    final List<Expr> arithmeticExpression =
                            processArithmeticExpression(functionCall.getArguments().get(0),
                                                        tablesReferencedInView,
                                                        isAggregate);
                    assert arithmeticExpression.size() == 1;
                    final Expr argument = arithmeticExpression.get(0);
                    function = new MonoidFunction(mnzFunctionName, argument);
                } else if (functionCall.getArguments().isEmpty() &&
                            "count".equalsIgnoreCase(functionCall.getName().getSuffix())) {
                    // The presto parser does not consider count(*) as a function with a single
                    // argument "*", but instead treats it as a function without any arguments.
                    // The parser code therefore has this special case behavior when it
                    // comes to the count function. See Presto's ExpressionFormatter.visitFunctionCall() for how
                    // this is handled externally from the FunctionCall code.
                    //
                    // We therefore replace the argument for count with the first column of one of the tables.
                    final IRTable table = tablesReferencedInView.iterator().next();
                    final IRColumn field = table.getIRColumns().entrySet().iterator().next().getValue();
                    final ColumnIdentifier column = new ColumnIdentifier(table.getName(), field, false);
                    function = new MonoidFunction(mnzFunctionName, column);
                } else {
                    throw new RuntimeException("I don't know what to do with this function call type: " + functionCall);
                }
                operands.push(function);
            } else if (node instanceof DereferenceExpression) {
                final ColumnIdentifier columnIdentifier = getColumnIdentifierFromField(node, tablesReferencedInView);
                operands.push(columnIdentifier);
            } else if (node instanceof Identifier) {
                final ColumnIdentifier identifier = getColumnIdentifierFromField(node, tablesReferencedInView);
                operands.push(identifier);
            } else if (isLiteral(node)) {
                final MonoidLiteral literal;
                if (node instanceof StringLiteral) {
                    literal = new MonoidLiteral<>("\'" + ((StringLiteral) node).getValue() + "\'",
                            String.class);
                } else if (node instanceof LongLiteral) {
                    literal = new MonoidLiteral<>(Long.valueOf(node.toString()), Long.class);
                } else if (node instanceof BooleanLiteral) {
                    literal = new MonoidLiteral<>(Boolean.valueOf(node.toString()), Boolean.class);
                } else {
                    throw new UnsupportedOperationException("I don't know how to handle this literal " + node);
                }
                operands.push(literal);
            } else if (node instanceof ArithmeticBinaryExpression) {
                assert operands.size() >= 2;
                final Expr left = operands.pop();
                final Expr right = operands.pop();
                final String operatorMnz = operatorTranslator(((ArithmeticBinaryExpression) node).getOperator());
                operands.push(createOperatorPredicate(operatorMnz, left, right, isAggregate));
            } else if (node instanceof ComparisonExpression) {
                assert operands.size() >= 2;
                final Expr left = operands.pop();
                final Expr right = operands.pop();
                final String operatorMnz = operatorTranslator(((ComparisonExpression) node).getOperator());
                operands.push(createOperatorPredicate(operatorMnz, left, right, isAggregate));
            } else if (node instanceof LogicalBinaryExpression) {
                assert operands.size() >= 2;
                final Expr left = operands.pop();
                final Expr right = operands.pop();
                final String operatorMnz = operatorTranslator(((LogicalBinaryExpression) node).getOperator());
                operands.push(createOperatorPredicate(operatorMnz, left, right, isAggregate));
            } else if (node instanceof InPredicate) {
                final Expr left = operands.pop();
                final Expr right = operands.pop();
                final BinaryOperatorPredicate operatorPredicate =
                        new BinaryOperatorPredicate("in", left, right);
                operands.push(operatorPredicate);
            } else if (node instanceof ExistsPredicate) {
                final Expr innerExpr = operands.pop();
                final org.dcm.compiler.monoid.ExistsPredicate operatorPredicate =
                        new org.dcm.compiler.monoid.ExistsPredicate(innerExpr);
                operands.push(operatorPredicate);
            } else if (node instanceof NotExpression) {
                final Expr innerExpr = operands.pop();
                final MonoidFunction operatorPredicate = new MonoidFunction("not", innerExpr);
                operands.push(operatorPredicate);
            } else if (node instanceof ArithmeticUnaryExpression) {
                final Expr innerExpr = operands.pop();
                final ArithmeticUnaryExpression.Sign sign = ((ArithmeticUnaryExpression) node).getSign();
                final String signStr = sign.equals(ArithmeticUnaryExpression.Sign.MINUS) ? "-" : "";
                final MonoidFunction operatorPredicate = new MonoidFunction(signStr, innerExpr);
                operands.push(operatorPredicate);
            } else {
                throw new IllegalArgumentException("Unknown type stack");
            }
        }
        return operands;
    }

    private BinaryOperatorPredicate createOperatorPredicate(final String operatorMnz, final Expr left,
                                                            final Expr right, final boolean isAggregate) {
        return isAggregate
                ? new BinaryOperatorPredicateWithAggregate(operatorMnz, left, right)
                : new BinaryOperatorPredicate(operatorMnz, left, right);
    }

    private ColumnIdentifier getColumnIdentifierFromField(final Node node, final Set<IRTable> tablesReferencedInView) {
          if (node instanceof DereferenceExpression) {
            final IRColumn irColumn = getIRColumnFromDereferencedExpression((DereferenceExpression) node);
            return new ColumnIdentifier(irColumn.getIRTable().getName(), irColumn, true);
        } else if (node instanceof Identifier) {
            final IRColumn irColumn = irContext.getColumnIfUnique(node.toString(), tablesReferencedInView);
            assert tablesReferencedInView.stream().map(IRTable::getName)
                                                  .collect(Collectors.toSet())
                                                  .contains(irColumn.getIRTable().getName());
            return new ColumnIdentifier(irColumn.getIRTable().getName(), irColumn, false);
        } else {
            throw new IllegalArgumentException("Unknown field type");
        }
    }

    private IRColumn getIRColumnFromDereferencedExpression(final DereferenceExpression node) {
        final List<String> identifier = Splitter.on(".")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(node.toString());

        // Only supports dereference expressions that have exactly 1 dot.
        // At the moment we don't support, e.g. schema.reference.field - that is we only support queries
        // within the same schema
        if (identifier.size() != 2) {
            throw new UnsupportedOperationException("Dereference fields can only be of the format `table.field`");
        }
        final String tableName = identifier.get(0);
        final String fieldName = identifier.get(1);
        return irContext.getColumn(tableName, fieldName);
    }

    private static String operatorTranslator(final ArithmeticBinaryExpression.Operator op) {
        return ARITHMETIC_OP_TABLE.get(op);
    }

    private static String operatorTranslator(final LogicalBinaryExpression.Operator op) {
        return LOGICAL_OP_TABLE.get(op);
    }

    private static String operatorTranslator(final ComparisonExpression.Operator op) {
        return COMPARISON_OP_TABLE.get(op);
    }

    private static boolean isLiteral(final Node node) {
        return node instanceof Literal;
    }
}