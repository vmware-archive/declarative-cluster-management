/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.base.Splitter;
import org.dcm.IRColumn;
import org.dcm.IRContext;
import org.dcm.IRTable;
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
import org.dcm.compiler.monoid.UnaryOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TranslateViewToIR extends DefaultTraversalVisitor<Optional<Expr>, Void> {
    private static final Map<ArithmeticBinaryExpression.Operator,
            BinaryOperatorPredicate.Operator> ARITHMETIC_OP_TABLE =
            new EnumMap<>(ArithmeticBinaryExpression.Operator.class);
    private static final Map<ComparisonExpression.Operator,
            BinaryOperatorPredicate.Operator> COMPARISON_OP_TABLE =
            new EnumMap<>(ComparisonExpression.Operator.class);
    private static final Map<LogicalBinaryExpression.Operator,
            BinaryOperatorPredicate.Operator> LOGICAL_OP_TABLE =
            new EnumMap<>(LogicalBinaryExpression.Operator.class);

    static {
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.ADD,
                BinaryOperatorPredicate.Operator.ADD);
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.SUBTRACT,
                BinaryOperatorPredicate.Operator.SUBTRACT);
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.MULTIPLY,
                BinaryOperatorPredicate.Operator.MULTIPLY);
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.DIVIDE,
                BinaryOperatorPredicate.Operator.DIVIDE);
        ARITHMETIC_OP_TABLE.put(ArithmeticBinaryExpression.Operator.MODULUS,
                BinaryOperatorPredicate.Operator.MODULUS);

        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.EQUAL,
                BinaryOperatorPredicate.Operator.EQUAL);
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.LESS_THAN,
                BinaryOperatorPredicate.Operator.LESS_THAN);
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.GREATER_THAN,
                BinaryOperatorPredicate.Operator.GREATER_THAN);
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                BinaryOperatorPredicate.Operator.LESS_THAN_OR_EQUAL);
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                BinaryOperatorPredicate.Operator.GREATER_THAN_OR_EQUAL);
        COMPARISON_OP_TABLE.put(ComparisonExpression.Operator.NOT_EQUAL,
                BinaryOperatorPredicate.Operator.NOT_EQUAL);

        LOGICAL_OP_TABLE.put(LogicalBinaryExpression.Operator.AND, BinaryOperatorPredicate.Operator.AND);
        LOGICAL_OP_TABLE.put(LogicalBinaryExpression.Operator.OR, BinaryOperatorPredicate.Operator.OR);
    }

    private final IRContext irContext;
    private final Set<IRTable> tablesReferencedInView;
    private final boolean isAggregate;

    TranslateViewToIR(final IRContext irContext, final Set<IRTable> tablesReferencedInView, final boolean isAggregate) {
        this.irContext = irContext;
        this.tablesReferencedInView = tablesReferencedInView;
        this.isAggregate = isAggregate;
    }

    private Expr translateExpression(final Expression expression) {
        return translateExpression(expression, irContext, tablesReferencedInView, isAggregate);
    }

    @Override
    protected Optional<Expr> visitLogicalBinaryExpression(final LogicalBinaryExpression node, final Void context) {
        final Expr left = translateExpression(node.getLeft());
        final Expr right = translateExpression(node.getRight());
        final BinaryOperatorPredicate.Operator operator = operatorTranslator(node.getOperator());
        return Optional.of(createOperatorPredicate(operator, left, right, isAggregate));
    }

    @Override
    protected Optional<Expr> visitComparisonExpression(final ComparisonExpression node, final Void context) {
        final Expr left = translateExpression(node.getLeft());
        final Expr right = translateExpression(node.getRight());
        final BinaryOperatorPredicate.Operator operator = operatorTranslator(node.getOperator());
        return Optional.of(createOperatorPredicate(operator, left, right, isAggregate));
    }

    @Override
    protected Optional<Expr> visitArithmeticBinary(final ArithmeticBinaryExpression node, final Void context) {
        final Expr left = translateExpression(node.getLeft());
        final Expr right = translateExpression(node.getRight());
        final BinaryOperatorPredicate.Operator operator = operatorTranslator(node.getOperator());
        return Optional.of(createOperatorPredicate(operator, left, right, isAggregate));
    }

    @Override
    protected Optional<Expr> visitArithmeticUnary(final ArithmeticUnaryExpression node, final Void context) {
        final Expr innerExpr = translateExpression(node.getValue());
        final ArithmeticUnaryExpression.Sign sign = node.getSign();
        final UnaryOperator.Operator signStr = sign.equals(ArithmeticUnaryExpression.Sign.MINUS) ?
                UnaryOperator.Operator.MINUS : UnaryOperator.Operator.PLUS;
        final UnaryOperator operatorPredicate = new UnaryOperator(signStr, innerExpr);
        return Optional.of(operatorPredicate);
    }

    @Override
    protected Optional<Expr> visitExists(final ExistsPredicate node, final Void context) {
        final Expr innerExpr = translateExpression(node.getSubquery());
        final org.dcm.compiler.monoid.ExistsPredicate operatorPredicate =
                new org.dcm.compiler.monoid.ExistsPredicate(innerExpr);
        return Optional.of(operatorPredicate);
    }

    @Override
    protected Optional<Expr> visitInPredicate(final InPredicate node, final Void context) {
        final Expr left = translateExpression(node.getValue());
        final Expr right = translateExpression(node.getValueList());
        final BinaryOperatorPredicate operatorPredicate =
                new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.IN, left, right);
        return Optional.of(operatorPredicate);
    }

    @Override
    protected Optional<Expr> visitFunctionCall(final FunctionCall node, final Void context) {
        if (node.getArguments().size() == 1
                || (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix()))
                || (node.getArguments().size() == 4 && node.getName().getSuffix().equals("capacity_constraint"))
                || (node.getArguments().size() == 2 && node.getName().getSuffix().equals("contains"))) {
            // Only having clauses will have function calls in the expression.
            final Expr function;
            final String functionNameStr = node.getName().toString().toUpperCase(Locale.US);
            final MonoidFunction.Function functionType = MonoidFunction.Function.valueOf(functionNameStr);
            if (node.getArguments().size() >= 1) {
                final List<Expr> arguments = node.getArguments().stream()
                        .map(e -> translateExpression(e, irContext, tablesReferencedInView, isAggregate))
                        .collect(Collectors.toList());
                function = new MonoidFunction(functionType, arguments);
            } else if (node.getArguments().isEmpty() &&
                    "count".equalsIgnoreCase(node.getName().getSuffix())) {
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
                function = new MonoidFunction(functionType, column);
            } else {
                throw new RuntimeException("I don't know what to do with this function call type: " + node);
            }
            return Optional.of(function);
        } else {
            throw new RuntimeException("I don't know what do with the following node: " + node);
        }
    }

    /**
     * Parse columns like 'reference.field'
     */
    @Override
    protected Optional<Expr> visitDereferenceExpression(final DereferenceExpression node, final Void context) {
        final IRColumn irColumn = getIRColumnFromDereferencedExpression(node, irContext);
        final ColumnIdentifier columnIdentifier = new ColumnIdentifier(irColumn.getIRTable().getName(), irColumn,
                                                true);
        return Optional.of(columnIdentifier);
    }

    @Override
    protected Optional<Expr> visitSubqueryExpression(final SubqueryExpression node, final Void context) {
        final Query subQuery = node.getQuery();
        return Optional.of(apply(subQuery, Optional.empty(), irContext));
    }

    @Override
    protected Optional<Expr> visitLiteral(final Literal node, final Void context) {
        return super.visitLiteral(node, context);
    }

    @Override
    protected Optional<Expr> visitStringLiteral(final StringLiteral node, final Void context) {
        return Optional.of(new MonoidLiteral<>("\'" + node.getValue() + "\'", String.class));
    }

    @Override
    protected Optional<Expr> visitLongLiteral(final LongLiteral node, final Void context) {
        return Optional.of(new MonoidLiteral<>(Long.valueOf(node.toString()), Long.class));
    }

    @Override
    protected Optional<Expr> visitBooleanLiteral(final BooleanLiteral node, final Void context) {
        return Optional.of(new MonoidLiteral<>(Boolean.valueOf(node.toString()), Boolean.class));
    }

    @Override
    protected Optional<Expr> visitIdentifier(final Identifier node, final Void context) {
        final IRColumn irColumn = irContext.getColumnIfUnique(node.toString(), tablesReferencedInView);
        assert tablesReferencedInView.stream().map(IRTable::getName)
                .collect(Collectors.toSet())
                .contains(irColumn.getIRTable().getName());
        final ColumnIdentifier identifier = new ColumnIdentifier(irColumn.getIRTable().getName(), irColumn, false);
        return Optional.of(identifier);
    }

    @Override
    protected Optional<Expr> visitNotExpression(final NotExpression node, final Void context) {
        final Expr innerExpr = translateExpression(node.getValue());
        final UnaryOperator operatorPredicate = new UnaryOperator(UnaryOperator.Operator.NOT, innerExpr);
        return Optional.of(operatorPredicate);
    }

    @Override
    protected Optional<Expr> visitIsNullPredicate(final IsNullPredicate node, final Void context) {
        final Expr innerExpr = translateExpression(node.getValue());
        final org.dcm.compiler.monoid.IsNullPredicate isNullPredicate =
                new org.dcm.compiler.monoid.IsNullPredicate(innerExpr);
        return Optional.of(isNullPredicate);
    }

    @Override
    protected Optional<Expr> visitIsNotNullPredicate(final IsNotNullPredicate node, final Void context) {
        final Expr innerExpr = translateExpression(node.getValue());
        final org.dcm.compiler.monoid.IsNotNullPredicate isNotNullPredicate =
                new org.dcm.compiler.monoid.IsNotNullPredicate(innerExpr);
        return Optional.of(isNotNullPredicate);
    }


    /**
     * Converts an SQL view into an IR list comprehension.
     *
     * @param view the AST corresponding to an SQL view statement
     * @param irContext an IRContext instance
     * @return A list comprehension corresponding to the view parameter
     */
    static MonoidComprehension apply(final Query view, final Optional<Expression> check, final IRContext irContext) {
        final FromExtractor fromParser = new FromExtractor(irContext);
        fromParser.process(view.getQueryBody());

        final Set<IRTable> tables = fromParser.getTables();
        final Optional<Expression> where = ((QuerySpecification) view.getQueryBody()).getWhere();
        final List<Expression> joinConditions = fromParser.getJoinConditions();
        final Optional<Expression> having = ((QuerySpecification) view.getQueryBody()).getHaving();
        final Optional<GroupBy> groupBy = ((QuerySpecification) view.getQueryBody()).getGroupBy();

        // Construct Monoid Comprehension
        final List<SelectItem> selectItems = ((QuerySpecification) view.getQueryBody()).getSelect().getSelectItems();
        final List<Expr> selectItemExpr = translateSelectItems(selectItems, irContext, tables);
        final Head head = new Head(selectItemExpr);

        final List<Qualifier> qualifiers = new ArrayList<>();
        tables.forEach(t -> qualifiers.add(new TableRowGenerator(t)));
        where.ifPresent(e -> qualifiers.add((Qualifier) translateExpression(e, irContext, tables, false)));
        having.ifPresent(e -> qualifiers.add((Qualifier) translateExpression(e, irContext, tables, true)));
        check.ifPresent(e -> qualifiers.add((Qualifier) translateExpression(e, irContext, tables, having.isPresent())));

        joinConditions.forEach(e -> {
            final Qualifier joinQualifier = (Qualifier) translateExpression(e, irContext, tables, false);
            assert joinQualifier instanceof BinaryOperatorPredicate;
            qualifiers.add(new JoinPredicate((BinaryOperatorPredicate) joinQualifier));
        });

        if (groupBy.isPresent()) {
            final List<GroupingElement> groupingElement = groupBy.get().getGroupingElements();
            final List<ColumnIdentifier> columnIdentifiers = columnListFromGroupBy(groupingElement, irContext, tables);
            final GroupByQualifier groupByQualifier = new GroupByQualifier(columnIdentifiers);
            final MonoidComprehension comprehension = new MonoidComprehension(head, qualifiers);
            return new GroupByComprehension(comprehension, groupByQualifier);
        }
        return new MonoidComprehension(head, qualifiers);
    }

    /**
     * Translates a list of SQL SelectItems into a corresponding list of IR Expr
     */
    private static List<Expr> translateSelectItems(final List<SelectItem> selectItems,
                                                   final IRContext irContext,
                                                   final Set<IRTable> tables) {
        final List<Expr> exprs = new ArrayList<>();
        for (final SelectItem selectItem: selectItems) {
            if (selectItem instanceof SingleColumn) {
                final SingleColumn singleColumn = (SingleColumn) selectItem;
                final Expression expression = singleColumn.getExpression();
                final Expr expr = translateExpression(expression, irContext, tables, false);
                singleColumn.getAlias().ifPresent(v -> expr.setAlias(v.toString()));
                exprs.add(expr);
            } else if (selectItem instanceof AllColumns) {
                tables.forEach(
                        table -> table.getIRColumns().forEach((fieldName, irColumn) -> {
                            exprs.add(new ColumnIdentifier(table.getName(), irColumn, false));
                        })
                );
            }
        }
        return exprs;
    }

    /**
     * Translates a list of SQL GroupingElements into a corresponding list of ColumnIdentifiers. This method does
     * not work for GroupBy expressions that are not over columns.
     */
    private static List<ColumnIdentifier> columnListFromGroupBy(final List<GroupingElement> groupingElements,
                                                                final IRContext irContext, final Set<IRTable> tables) {
        return groupingElements.stream()
                .map(e -> ((SimpleGroupBy) e).getColumnExpressions()) // We only support SimpleGroupBy
                .flatMap(Collection::stream)
                .map(expr -> translateExpression(expr, irContext, tables, false))
                .map(expr -> (ColumnIdentifier) expr) // We only support columns as group by elements
                .collect(Collectors.toList());
    }

    /**
     * Translates an SQL AST expression into an IR Expr type.
     */
    private static Expr translateExpression(final Expression expression, final IRContext irContext,
                                           final Set<IRTable> tablesReferencedInView, final boolean isAggregate) {
        final TranslateViewToIR traverser = new TranslateViewToIR(irContext, tablesReferencedInView, isAggregate);
        return traverser.process(expression).orElseThrow();
    }

    private static BinaryOperatorPredicate createOperatorPredicate(final BinaryOperatorPredicate.Operator operator,
                                                                   final Expr left, final Expr right,
                                                                   final boolean isAggregate) {
        return isAggregate
                ? new BinaryOperatorPredicateWithAggregate(operator, left, right)
                : new BinaryOperatorPredicate(operator, left, right);
    }

    /**
     * Retrieve an IRColumn from a given IRContext that corresponds to a DerefenceExpression node from the SQL AST
     */
    static IRColumn getIRColumnFromDereferencedExpression(final DereferenceExpression node, final IRContext irContext) {
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

    private static BinaryOperatorPredicate.Operator operatorTranslator(final ArithmeticBinaryExpression.Operator op) {
        return ARITHMETIC_OP_TABLE.get(op);
    }

    private static BinaryOperatorPredicate.Operator operatorTranslator(final LogicalBinaryExpression.Operator op) {
        return LOGICAL_OP_TABLE.get(op);
    }

    private static BinaryOperatorPredicate.Operator operatorTranslator(final ComparisonExpression.Operator op) {
        return COMPARISON_OP_TABLE.get(op);
    }
}