/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicateWithAggregate;
import com.vmware.dcm.compiler.ir.CheckQualifier;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.GroupByQualifier;
import com.vmware.dcm.compiler.ir.Head;
import com.vmware.dcm.compiler.ir.IsNotNullPredicate;
import com.vmware.dcm.compiler.ir.IsNullPredicate;
import com.vmware.dcm.compiler.ir.JoinPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.Qualifier;
import com.vmware.dcm.compiler.ir.TableRowGenerator;
import com.vmware.dcm.compiler.ir.UnaryOperator;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TranslateViewToIR extends SqlBasicVisitor<Optional<Expr>> {
    private static final Map<SqlKind, BinaryOperatorPredicate.Operator> OP_TABLE = new EnumMap<>(SqlKind.class);

    static {
        OP_TABLE.put(SqlKind.PLUS, BinaryOperatorPredicate.Operator.ADD);
        OP_TABLE.put(SqlKind.MINUS, BinaryOperatorPredicate.Operator.SUBTRACT);
        OP_TABLE.put(SqlKind.TIMES, BinaryOperatorPredicate.Operator.MULTIPLY);
        OP_TABLE.put(SqlKind.DIVIDE, BinaryOperatorPredicate.Operator.DIVIDE);
        OP_TABLE.put(SqlKind.MOD, BinaryOperatorPredicate.Operator.MODULUS);

        OP_TABLE.put(SqlKind.EQUALS, BinaryOperatorPredicate.Operator.EQUAL);
        OP_TABLE.put(SqlKind.LESS_THAN, BinaryOperatorPredicate.Operator.LESS_THAN);
        OP_TABLE.put(SqlKind.GREATER_THAN, BinaryOperatorPredicate.Operator.GREATER_THAN);
        OP_TABLE.put(SqlKind.LESS_THAN_OR_EQUAL, BinaryOperatorPredicate.Operator.LESS_THAN_OR_EQUAL);
        OP_TABLE.put(SqlKind.GREATER_THAN_OR_EQUAL, BinaryOperatorPredicate.Operator.GREATER_THAN_OR_EQUAL);
        OP_TABLE.put(SqlKind.NOT_EQUALS, BinaryOperatorPredicate.Operator.NOT_EQUAL);

        OP_TABLE.put(SqlKind.IN, BinaryOperatorPredicate.Operator.IN);

        OP_TABLE.put(SqlKind.AND, BinaryOperatorPredicate.Operator.AND);
        OP_TABLE.put(SqlKind.OR, BinaryOperatorPredicate.Operator.OR);
    }

    private final IRContext irContext;
    private final Set<IRTable> tablesReferencedInView;
    private final boolean isAggregate;

    TranslateViewToIR(final IRContext irContext, final Set<IRTable> tablesReferencedInView, final boolean isAggregate) {
        this.irContext = irContext;
        this.tablesReferencedInView = tablesReferencedInView;
        this.isAggregate = isAggregate;
    }

    private Expr translateExpression(final SqlNode expression) {
        return translateExpression(expression, irContext, tablesReferencedInView, isAggregate);
    }

    @Override
    public Optional<Expr> visit(final SqlCall call) {
        if (call.getOperator() instanceof SqlBinaryOperator) {
            return visitBinaryExpression(call);
        } else if (call.getOperator() instanceof SqlPrefixOperator) {
            return visitPrefixOperator(call);
        } else if (call.getOperator() instanceof SqlPostfixOperator) {
            return visitPostfixOperator(call);
        } else if (call.getOperator() instanceof SqlUnresolvedFunction) {
            return visitFunctionCall(call);
        } else if (call.getOperator() instanceof SqlAsOperator) {
            final SqlNode operand = call.operand(0);
            final String alias = call.operand(1).toString();
            final Expr inner = translateExpression(operand);
            inner.setAlias(alias);
            return Optional.of(inner);
        } else if (call instanceof SqlSelect) {
            return visitSubqueryExpression(call);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Expr> visit(final SqlIdentifier id) {
        if (id.names.size() == 1) {
            return visitIdentifier(id);
        } else if (id.names.size() == 2) {
            return visitDereferenceExpression(id);
        } else {
            throw new IllegalArgumentException(id.toString());
        }
    }

    @Override
    public Optional<Expr> visit(final SqlLiteral literal) {
        switch (literal.getTypeName()) {
            case DECIMAL:
                return Optional.of(new Literal<>(Long.valueOf(literal.toString()), Long.class));
            case CHAR:
                final Object value = literal.getValue();
                assert value != null;
                return Optional.of(new Literal<>(value.toString(), String.class));
            case BOOLEAN:
                return Optional.of(new Literal<>(Boolean.valueOf(literal.toString()), Boolean.class));
            default:
                throw new IllegalArgumentException(literal.toString());
        }
    }

    protected Optional<Expr> visitBinaryExpression(final SqlCall node) {
        assert node.operandCount() == 2;
        final Expr left = translateExpression(node.operand(0));
        final Expr right = translateExpression(node.operand(1));

        // Translated A NOT IN B to NOT(A IN B)
        if (node.getOperator().getKind() == SqlKind.NOT_IN) {
            final BinaryOperatorPredicate.Operator inOperator = operatorTranslator(SqlKind.IN);
            final BinaryOperatorPredicate inOperation = createOperatorPredicate(inOperator, left, right, isAggregate);
            final UnaryOperator notInOperation = new UnaryOperator(UnaryOperator.Operator.NOT, inOperation);
            return Optional.of(notInOperation);
        }
        final BinaryOperatorPredicate.Operator operator = operatorTranslator(node.getOperator().getKind());
        return Optional.of(createOperatorPredicate(operator, left, right, isAggregate));
    }

    protected Optional<Expr> visitPrefixOperator(final SqlCall node) {
        final Expr innerExpr = translateExpression(node.operand(0));
        switch (node.getOperator().getKind()) {
            case MINUS_PREFIX:
                return Optional.of(new UnaryOperator(UnaryOperator.Operator.MINUS, innerExpr));
            case PLUS_PREFIX:
                return Optional.of(new UnaryOperator(UnaryOperator.Operator.PLUS, innerExpr));
            case NOT:
                return Optional.of(new UnaryOperator(UnaryOperator.Operator.NOT, innerExpr));
            case EXISTS:
                return visitExists(node);
            default:
                throw new IllegalArgumentException(node.toString());
        }
    }

    protected Optional<Expr> visitExists(final SqlCall node) {
        final Expr innerExpr = translateExpression(node.operand(0));
        final ExistsPredicate operatorPredicate = new ExistsPredicate(innerExpr);
        return Optional.of(operatorPredicate);
    }

    protected Optional<Expr> visitFunctionCall(final SqlCall node) {
        // Only having clauses will have function calls in the expression.
        final Expr function;
        final String functionNameStr = node.getOperator().getName().toUpperCase(Locale.US);
        final FunctionCall.Function functionType = FunctionCall.Function.valueOf(functionNameStr);
        if (node.getOperator().getName().equalsIgnoreCase("count")
            && ((SqlIdentifier) node.operand(0)).isStar()) {
            // Replace the argument for count with the first column of one of the tables.
            final IRTable table = tablesReferencedInView.iterator().next();
            final IRColumn field = table.getIRColumns().entrySet().iterator().next().getValue();
            final ColumnIdentifier column = new ColumnIdentifier(table.getName(), field, false);
            function = new FunctionCall(functionType, column);
        } else if (node.operandCount() >= 1) {
            final List<Expr> arguments = node.getOperandList().stream()
                    .map(e -> translateExpression(e, irContext, tablesReferencedInView, isAggregate))
                    .collect(Collectors.toList());
            function = new FunctionCall(functionType, arguments);
        } else {
            throw new RuntimeException("I don't know what to do with this function call type: " + node);
        }
        return Optional.of(function);
    }

    /**
     * Parse columns like 'reference.field'
     */
    protected Optional<Expr> visitDereferenceExpression(final SqlIdentifier node) {
        final IRColumn irColumn = getIRColumnFromDereferencedExpression(node, irContext);
        final ColumnIdentifier columnIdentifier = new ColumnIdentifier(irColumn.getIRTable().getName(), irColumn,
                                                true);
        return Optional.of(columnIdentifier);
    }

    protected Optional<Expr> visitSubqueryExpression(final SqlCall node) {
        return Optional.of(apply((SqlSelect) node, Optional.empty(), irContext));
    }

    protected Optional<Expr> visitIdentifier(final SqlIdentifier node) {
        final IRColumn irColumn = irContext.getColumnIfUnique(node.toString(), tablesReferencedInView);
        assert tablesReferencedInView.stream().map(IRTable::getName)
                .collect(Collectors.toSet())
                .contains(irColumn.getIRTable().getName());
        final ColumnIdentifier identifier = new ColumnIdentifier(irColumn.getIRTable().getName(), irColumn, false);
        return Optional.of(identifier);
    }

    protected Optional<Expr> visitPostfixOperator(final SqlCall node) {
        final Expr innerExpr = translateExpression(node.operand(0));
        switch (node.getOperator().getKind()) {
            case IS_NOT_NULL:
                return Optional.of(new IsNotNullPredicate(innerExpr));
            case IS_NULL:
                return Optional.of(new IsNullPredicate(innerExpr));
            default:
                throw new IllegalArgumentException(node.toString());
        }
    }

    /**
     * Converts an SQL view into an IR list comprehension.
     *
     * @param select the AST corresponding to an SQL view statement
     * @param irContext an IRContext instance
     * @return A list comprehension corresponding to the view parameter
     */
    static ListComprehension apply(final SqlSelect select, final Optional<SqlNode> constraint,
                                   final IRContext irContext) {
        final FromExtractor fromParser = new FromExtractor(irContext);
        select.accept(fromParser);

        final Set<IRTable> tables = fromParser.getTables();
        final Optional<SqlNode> where = Optional.ofNullable(select.getWhere());
        final List<SqlNode> joinConditions = fromParser.getJoinConditions();
        final Optional<SqlNode> having = Optional.ofNullable(select.getHaving());
        final Optional<SqlNodeList> groupBy = Optional.ofNullable(select.getGroup());

        // Construct Monoid Comprehension
        final SqlNodeList selectItems = select.getSelectList();
        final List<Expr> selectItemExpr = translateSelectItems(selectItems, irContext, tables);
        final Head head = new Head(selectItemExpr);

        final List<Qualifier> qualifiers = new ArrayList<>();
        tables.forEach(t -> qualifiers.add(new TableRowGenerator(t)));
        where.ifPresent(e -> qualifiers.add(toQualifier(translateExpression(e, irContext, tables, false))));
        having.ifPresent(e -> qualifiers.add(toQualifier(translateExpression(e, irContext, tables, true))));
        final UsesAggregateFunctions usesAggregateFunctions = new UsesAggregateFunctions();
        constraint.ifPresent(e -> {
            e.accept(usesAggregateFunctions);
            final Expr expr = translateExpression(e, irContext, tables, usesAggregateFunctions.isFound()
                                                      || groupBy.isPresent() || having.isPresent());
            qualifiers.add(new CheckQualifier(expr));
        });

        joinConditions.forEach(e -> {
            final Qualifier joinQualifier = toQualifier(translateExpression(e, irContext, tables, false));
            assert joinQualifier instanceof BinaryOperatorPredicate;
            qualifiers.add(new JoinPredicate((BinaryOperatorPredicate) joinQualifier));
        });

        if (groupBy.isPresent()) {
            final List<Expr> columnIdentifiers = columnListFromGroupBy(groupBy.get(), irContext, tables);
            final GroupByQualifier groupByQualifier = new GroupByQualifier(columnIdentifiers);
            final ListComprehension comprehension = new ListComprehension(head, qualifiers);
            return new GroupByComprehension(comprehension, groupByQualifier);
        } else if (usesAggregateFunctions.isFound() || having.isPresent()) { // group by 1
            final GroupByQualifier groupByQualifier = new GroupByQualifier(
                    List.of(new Literal<>(1, Integer.class)));
            final ListComprehension comprehension = new ListComprehension(head, qualifiers);
            return new GroupByComprehension(comprehension, groupByQualifier);
        }
        return new ListComprehension(head, qualifiers);
    }

    private static Qualifier toQualifier(final Expr expr) {
        return expr instanceof Qualifier ? (Qualifier) expr :
                new BinaryOperatorPredicate(BinaryOperatorPredicate.Operator.EQUAL, expr,
                        new Literal<>(true, Boolean.class));
    }

    /**
     * Translates a list of SQL SelectItems into a corresponding list of IR Expr
     */
    private static List<Expr> translateSelectItems(final SqlNodeList selectItems,
                                                   final IRContext irContext,
                                                   final Set<IRTable> tables) {
        final List<Expr> exprs = new ArrayList<>();
        for (final SqlNode selectItem: selectItems) {
            if (selectItem instanceof SqlIdentifier && ((SqlIdentifier) selectItem).isStar()) {
                tables.forEach(
                        table -> table.getIRColumns().forEach((fieldName, irColumn) ->
                                exprs.add(new ColumnIdentifier(table.getName(), irColumn, false)))
                );
            } else {
                final Expr expr = translateExpression(selectItem, irContext, tables, false);
                exprs.add(expr);
            }
        }
        return exprs;
    }

    /**
     * Translates a list of SQL GroupingElements into a corresponding list of ColumnIdentifiers. This method does
     * not work for GroupBy expressions that are not over columns or constant expressions.
     */
    private static List<Expr> columnListFromGroupBy(final SqlNodeList groupingElements,
                                                    final IRContext irContext, final Set<IRTable> tables) {
        return groupingElements.stream()
                .map(expr -> translateExpression(expr, irContext, tables, false))
                .collect(Collectors.toList());
    }

    /**
     * Translates an SQL AST expression into an IR Expr type.
     */
    private static Expr translateExpression(final SqlNode expression, final IRContext irContext,
                                           final Set<IRTable> tablesReferencedInView, final boolean isAggregate) {
        final TranslateViewToIR traverser = new TranslateViewToIR(irContext, tablesReferencedInView, isAggregate);
        final Optional<Expr> expr = expression.accept(traverser);
        return expr.orElseThrow();
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
    static IRColumn getIRColumnFromDereferencedExpression(final SqlIdentifier node, final IRContext irContext) {
        final List<String> identifier = node.names;

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

    private static BinaryOperatorPredicate.Operator operatorTranslator(final SqlKind op) {
        return Objects.requireNonNull(OP_TABLE.get(op));
    }
}