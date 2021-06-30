/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.minizinc;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.vmware.dcm.compiler.IRColumn;
import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.compiler.IRForeignKey;
import com.vmware.dcm.compiler.IRPrimaryKey;
import com.vmware.dcm.compiler.IRTable;
import com.vmware.dcm.compiler.UsesControllableFields;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicateWithAggregate;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.ExistsPredicate;
import com.vmware.dcm.compiler.ir.Expr;
import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.GroupByQualifier;
import com.vmware.dcm.compiler.ir.Head;
import com.vmware.dcm.compiler.ir.JoinPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.Literal;
import com.vmware.dcm.compiler.ir.Qualifier;
import com.vmware.dcm.compiler.ir.SimpleVisitor;
import com.vmware.dcm.compiler.ir.TableRowGenerator;
import com.vmware.dcm.compiler.ir.UnaryOperator;
import com.vmware.dcm.compiler.ir.VoidType;
import org.apache.commons.text.StringEscapeUtils;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Outputs code in the Minizinc language (http://minizinc.org/).
 */
public class MinizincCodeGenerator extends SimpleVisitor {
    private static final String GROUP_KEY = "GROUP__KEY";
    private static final EnumMap<VarType, String> VAR_TYPE_STRING = new EnumMap<>(VarType.class);
    private static final Map<FunctionCall.Function, String> FUNCTION_STRING_MAP =
            new EnumMap<>(FunctionCall.Function.class);

    private final List<Expr> headItems;
    private final List<BinaryOperatorPredicate> whereQualifiers;
    private final List<JoinPredicate> joinQualifiers;
    @Nullable
    private GroupByQualifier groupByQualifier = null;
    private final List<BinaryOperatorPredicateWithAggregate> aggregateQualifiers;
    private final List<String> rangeQualifiers;
    private final List<Expr> literals;
    private final List<Expr> completeExpression;
    private final String viewName;

    static {
        for (final FunctionCall.Function f: FunctionCall.Function.values()) {
            FUNCTION_STRING_MAP.put(f, f.toString().toLowerCase(Locale.US));
        }
    }

    MinizincCodeGenerator() {
        this("");
    }

    MinizincCodeGenerator(final String viewName) {
        this.headItems = new ArrayList<>();
        this.whereQualifiers = new ArrayList<>();
        this.joinQualifiers = new ArrayList<>();
        this.aggregateQualifiers = new ArrayList<>();
        this.rangeQualifiers = new ArrayList<>();
        this.literals = new ArrayList<>();
        this.completeExpression = new ArrayList<>();
        this.viewName = viewName;
    }

    @Override
    public VoidType visit(final Expr expr) {
        completeExpression.add(expr);
        return super.visit(expr);
    }

    @Override
    protected VoidType visitTableRowGenerator(final TableRowGenerator node, final VoidType context) {
        final String format = String.format("%s__ITER in 1..%s", node.getTable().getAliasedName(),
                                                                 MinizincString.tableNumRowsName(node.getTable()));
        rangeQualifiers.add(format);
        return defaultReturn();
    }

    @Override
    protected VoidType visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final VoidType context) {
        if (node instanceof BinaryOperatorPredicateWithAggregate) {
            aggregateQualifiers.add((BinaryOperatorPredicateWithAggregate) node);
        } else if (node instanceof JoinPredicate) {
            joinQualifiers.add((JoinPredicate) node);
        } else {
            whereQualifiers.add(node);
        }
        return defaultReturn();
    }

    @Override
    protected VoidType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
        literals.add(node);
        return defaultReturn();
    }

    @Override
    protected VoidType visitLiteral(final Literal node, final VoidType context) {
        literals.add(node);
        return defaultReturn();
    }

    @Override
    protected VoidType visitGroupByComprehension(final GroupByComprehension node, final VoidType context) {
        groupByQualifier = node.getGroupByQualifier();
        visit(node.getComprehension());
        return defaultReturn();
    }

    @Override
    protected VoidType visitUnaryOperator(final UnaryOperator node, final VoidType context) {
        literals.add(node);
        return defaultReturn();
    }

    @Override
    protected VoidType visitFunctionCall(final FunctionCall node, final VoidType context) {
        literals.add(node);
        return defaultReturn();
    }

    @Override
    protected VoidType visitExistsPredicate(final ExistsPredicate existsPredicate, final VoidType context) {
        literals.add(existsPredicate);
        return defaultReturn();
    }

    @Override
    protected VoidType visitHead(final Head node, final VoidType context) {
        headItems.addAll(node.getSelectExprs());
        return defaultReturn();
    }

    private List<String> evaluateWhereExpressions(final List<? extends BinaryOperatorPredicate> nodes) {
        return nodes.stream().map(this::evaluateWhereExpression).collect(Collectors.toList());
    }

    private String evaluateWhereExpression(final BinaryOperatorPredicate node) {
        final MinizincCodeGenerator leftVisitor = new MinizincCodeGenerator(viewName);
        final MinizincCodeGenerator rightVisitor = new MinizincCodeGenerator(viewName);
        leftVisitor.visit(node.getLeft());
        rightVisitor.visit(node.getRight());
        final List<String> leftExpression = leftVisitor.evaluateExpression();
        final List<String> rightExpression = rightVisitor.evaluateExpression();
        assert leftExpression.size() == 1;
        assert rightExpression.size() == 1;
        final String leftString = leftExpression.get(0);
        final String rightString = rightExpression.get(0);

        // Minizinc 'in' operators only work against sets. We replace it instead with the global
        // constraint member(<array of type T>, instance of T);
        if (node.getOperator().equals(BinaryOperatorPredicate.Operator.IN)) {
            return String.format("member(%s, %s)", rightString, leftString);
        }
        return String.format("(%s) %s (%s)", leftString, operatorToString(node.getOperator()), rightString);
    }

    List<String> generateArrayDeclarations(final IRContext context) {
        final List<String> ret = new ArrayList<>();

        // Tables
        for (final IRTable table: context.getTables()) {
            ret.add(String.format("%% %s Table", table.getName()));
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            ret.add(String.format("int: %s;", MinizincString.tableNumRowsName(table)));

            // Fields
            for (final Map.Entry<String, IRColumn> fieldEntrySet: table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();
                ret.add(String.format("%% %s", fieldName));
                ret.add(String.format("array[1..%s] %s %s : %s;",
                        MinizincString.tableNumRowsName(table),
                        field.isControllable() ? "of var" : "of",
                        MinizincString.typeName(field.getType()),
                        MinizincString.qualifiedName(field)));
            }

            table.getPrimaryKey()
                 .ifPresent(pk -> ret.add(getPrimaryKeyDeclaration(pk)));

            table.getForeignKeys()
                 .forEach(fk -> ret.add(getForeignKeyDeclaration(fk)));

            ret.add(getOutputStatementForTable(table));
        }
        return ret;
    }

    private String getOutputStatementForTable(final IRTable table) {
        if (table.isAliasedTable() || table.isViewTable()) {
            return "";
        }
        final String csvDelimiter = String.valueOf(MinizincString.MNZ_OUTPUT_CSV_DELIMITER);

        final List<String> shows = new ArrayList<>(table.getIRColumns().size());
        for (final Map.Entry<String, IRColumn> entry: table.getIRColumns().entrySet()) {
            final IRColumn column = entry.getValue();
            if (column.isString()) {
                // Bug with accessing array of strings using int vars. Used `fix` function per:
                // https://github.com/MiniZinc/MiniZincIDE/issues/35#issuecomment-304543235
                shows.add(String.format("show(to_enum(STRING_LITERALS, fix(%s[k])))",
                        MinizincString.qualifiedName(column)
                ));
            } else {
                shows.add(String.format("show(%s[k])", MinizincString.qualifiedName(column)));
            }
        }

        return String.format("output [ \"%s%s\" ++ \"\\n\" ++ \"%s\" ++ \"\\n\" ] ++ " +
                        "[ %s ++ \"\\n\" | k in 1..%s ] ++ [ \"\\n\" ];",
                // tag that separates tables and the name of the new table
                StringEscapeUtils.escapeJava(MinizincString.MNZ_OUTPUT_TABLENAME_TAG), table.getName(),
                // header of the CSV-like output with all the table fields
                table.getIRColumns().values().stream().map(IRColumn::getName)
                                             .collect(Collectors.joining(csvDelimiter)),
                // a MiniZinc show() for each column
                String.join(" ++ \"" + csvDelimiter + "\" ++ ", shows),
                // table rows names
                MinizincString.tableNumRowsName(table)
        );
    }


    /**
     * Returns a string of declarations for a non-constraint view. An example of how this works:
     * <p>
     * Let's say we have a view of the following form:
     * <p>
     * create view view1 as
     * select * from table1
     * join table2
     * on table1.col1 == table2.col1
     * <p>
     * This results in the following Minizinc:
     * <p>
     * % 1. Assume this is how the columns from the two different tables look:
     * array[int] of int: table1_col1 = [5, 4, 10, 6, 2, 100];
     * array[int] of int: table2_col1 = [1, 2, 4, 3, 6];
     * <p>
     * % 2. We pull out the columns for each table and column that are reflected in the join
     * % 4. For the same view as before, but the rows from table2 that appear in the final view
     * array[int] of var int: view1_table1_col1 = [ table1_col1[i] | i in 1..6, j in 1..5,
     * where table1_col1[i] == table2_col1[j]];
     * array[int] of var int: view1_table2_col1 = [ table1_col1[j] | i in 1..6, j in 1..5,
     * where table1_col1[i] == table2_col1[j]];
     */
    List<String> generateNonConstraintViewCode(final String viewName, final boolean generateArrayDeclaration) {
        final List<String> ret = new ArrayList<>();
        assert !headItems.isEmpty();
        /*
         * Expressions with a head are list comprehensions. Minizinc does not support
         * multiple head items in a comprehension, so we produce one comprehension per head item.
         */
        final VarType qualifiersType = usesControllableVariables(completeExpression.get(0));
        for (final Expr expr : headItems) {
            final String finalExpression = generatorExpressionFromSelectWhere(expr, false);
            assert !finalExpression.isEmpty();
            final VarType headType = usesControllableVariables(expr);
            final String declaration = String.format("array[int] of %s: %s__%s = %s;",
                    VAR_TYPE_STRING.get(getMax(headType, qualifiersType)),
                    viewName.toUpperCase(Locale.US),
                    MinizincString.headItemVariableName(expr),
                    finalExpression);
            ret.add(declaration);
        }
        if (generateArrayDeclaration) {
            final String headItemVariableName = MinizincString.headItemVariableName(headItems.get(0));
            final String viewNameUpper = viewName.toUpperCase(Locale.US);
            ret.add(String.format("int: %s = length(%s__%s);", MinizincString.tableNumRowsName(viewNameUpper),
                    viewNameUpper, headItemVariableName));
        }
        return ret;
    }

    /**
     * This is a similar structure to non-constraint views, but without the intermediate variable
     * declarations.
     */
    List<String> generateObjectiveFunctionCode(final String viewName) {
        final List<String> ret = new ArrayList<>();
        if (!headItems.isEmpty()) {
            /*
             * Expressions with a head are list comprehensions. Minizinc does not support
             * multiple head items in a comprehension, so we produce one comprehension per head item.
             */
            for (final Expr expr: headItems) {
                final String finalExpression = generatorExpressionFromSelectWhere(expr, false);
                assert !finalExpression.isEmpty();
                final String declaration = String.format("%s",
                        finalExpression);
                ret.add(declaration);
            }
        }
        return ret;
    }

    /**
     * Returns a string of declarations for a non-constraint view. An example of how this works:
     */
    List<String> generateConstraintViewCode(final String viewName) {
        final List<String> ret = new ArrayList<>();
        final String viewNameUpper = viewName.toUpperCase(Locale.US);
        final String expression = generateConstraintViewCodeInner("forall").get(0);
        ret.add(String.format("%% Constraint %s%nconstraint %s;", viewNameUpper, expression));
        return ret;
    }

    /**
     * Returns a string of declarations for a non-constraint view. An example of how this works:
     */
    private List<String> generateConstraintViewCodeInner(final String outerPredicate) {
        final List<String> ret = new ArrayList<>();
        if (!headItems.isEmpty()) {
            /*
             * While outer headStrings are required, we do not use them.
             */
            if (aggregateQualifiers.isEmpty()) {
                assert groupByQualifier == null;
                final String whereExpression = String.join(" /\\ ",
                        evaluateWhereExpressions(whereQualifiers));
                final String joinExpression = joinQualifiers.isEmpty() ? "" :
                        " where " + String.join(" /\\ ", evaluateWhereExpressions(joinQualifiers));
                final String rangeExpression = String.join(",", rangeQualifiers);
                // TODO: this special case is a sign that it is not safe to ignore headItems
                if (outerPredicate.equalsIgnoreCase("exists")) {
                    Preconditions.checkArgument(headItems.size() == 1);
                    Preconditions.checkArgument(headItems.get(0) instanceof BinaryOperatorPredicate);
                    final List<BinaryOperatorPredicate> exprs = new ArrayList<>();
                    exprs.add((BinaryOperatorPredicate) headItems.get(0));
                    exprs.addAll(whereQualifiers);
                    final String inner = String.join(" /\\ ", evaluateWhereExpressions(exprs));
                    ret.add(String.format("%s(%s %s)(%s)", outerPredicate, rangeExpression, joinExpression,
                            inner));
                } else {
                    ret.add(String.format("%s(%s %s)(%s)", outerPredicate, rangeExpression, joinExpression,
                            whereExpression));
                }
            } else {
                // Here, we write out the having clause with the required generator expression
                // applied to the columns on which the aggregate functions are applied
                for (final BinaryOperatorPredicateWithAggregate predicate: aggregateQualifiers) {
                    final String havingClause = evaluateHavingClause(predicate);
                    if (groupByQualifier == null) {
                        ret.add(String.format("%s", havingClause));
                    } else {
                        // For expressions with a group by, we output a constraint that ensures that
                        // the having clause is satisfied for every group.
                        final String groupByExpression =
                                String.format("%s(%s in 1..GROUP_TABLE__%s__NUM_ROWS)",
                                              outerPredicate, GROUP_KEY, viewName.toUpperCase(Locale.getDefault()));
                        ret.add(String.format("%s%n(%s)", groupByExpression, havingClause));
                    }
                }
            }
        }
        return ret;
    }


    /**
     * Return a single array comprehension for a select-where-groupby-having statement that has been already
     * parsed. Given that minizinc only supports a single head item per comprehension, we generate a comprehension
     * for a given input column.
     *
     * @param innerOnly    if true, returns only the inner expression that contains the group by key. Else,
     *                     it returns a single comprehension with an outer comprehension iterates over the groups,
     *                     and the inner comprehension with a group by predicate.
     * @return a String representing a Minizinc array comprehension
     */
    private String generatorExpressionFromSelectWhere(final Expr headItem,
                                                      final boolean innerOnly) {
        // the where, join and range expressions are common pieces regardless of whether
        // there is a group by involved.

        // We need a " where " clause in a Minizinc comprehension only if we have either whereQualifiers
        // or joinQualifiers. If they both exist, we need to get the conjunction of them. The final
        // result is held in the whereExpression variable.

        final List<BinaryOperatorPredicate> allQualifiers = new ImmutableList.Builder<BinaryOperatorPredicate>()
                .addAll(whereQualifiers)
                .addAll(joinQualifiers).build();
        final String whereExpression = allQualifiers.isEmpty() ? ""
                : String.join(" /\\ ", evaluateWhereExpressions(allQualifiers));
        final String rangeExpression = String.join(",", rangeQualifiers);
        if (groupByQualifier != null) {
            return handleGroupBy(headItem, innerOnly, rangeExpression, whereExpression, viewName);
        } else {
            final String body = String.format("%s %s %s", rangeExpression,
                                              whereExpression.isEmpty() ? "" : "where",
                                              whereExpression);
            // TODO: this a sign that we need to improve how functions are represented. They should not be head items.
            //  within the inner comprehensions
            if (headItem instanceof FunctionCall ||
                (headItem instanceof UnaryOperator &&
                    ((UnaryOperator) headItem).getArgument() instanceof FunctionCall)) {
                return evaluateHeadItem(headItem, body);
            }
            else {
                return String.format("[%s | %s]", evaluateHeadItem(headItem), body);
            }
        }
    }

    /**
     * Given a group by, we produce a comprehension of the form:
     * [<aggregate function>([<selectColumn> | <where clause> and <group predicate>]) | group in <groups>]
     *
     * Example: select count(col2) from table1
     *              where table1.col3 == 10
     *              group by col2
     *              having sum(col1) > 10;
     *
     * becomes...
     *
     *        [count(col2_gen_exp) |  group_index in uniqueTuples1(col2),
     *                                col2_gen_exp=[col2[i] | i in 1..table1_rows
     *                                              where col3 == 10 /\ col2[i] == col1[group_index],
     *                                col1_gen_exp=[col1[i] | i in 1..table1_rows
     *                                              where col3 == 10 /\ col2[i] == col1[group_index],
     *                                where sum(col1_gen_exp) > 10]
     *
     * The outer comprehension iterates over the groups, whereas the inner comprehension
     * iterates over all rows corresponding to a group. We assign the inner comprehension to an
     * intermediate variable named <col>_gen_exp. The inner group therefore needs a
     * group-by-predicate that is true if a row belongs to a particular group.
     *
     * The outer comprehension gives us indices in the table we are iterating on that correspond to
     * unique groups. This index is called the "group_index". The inner comprehension then checks,
     * for every column in the group by, whether the current row matches with that of the group_index.
     */
    private String handleGroupBy(final Expr headItem, final boolean innerOnly, final String rangeExpression,
                                 final String whereExpression, final String viewName) {
        assert groupByQualifier != null;
        final int numColumnsInGroupBy = groupByQualifier.getGroupByExprs().size();
        final List<String> groupByPredicates = new ArrayList<>(numColumnsInGroupBy);
        for (final ColumnIdentifier id : groupByQualifier.getGroupByColumnIdentifiers()) {
            final String columnWithIteration = MinizincString.columnNameWithIteration(id);
            final String groupColumnNameWithIteration = MinizincString.groupColumnNameWithIteration(viewName, id);
            final String groupByPredicate = String.format("%s == %s", columnWithIteration,
                                                                      groupColumnNameWithIteration);
            groupByPredicates.add(groupByPredicate);
        }
        final String groupByPredicatesString = String.join(" /\\ ", groupByPredicates);
        final String innerComprehensionQualifiers = String.format("%s %s %s %s %s",
                rangeExpression,
                // Add a where statement after all range qualifiers
                !whereExpression.isEmpty() || !groupByPredicatesString.isEmpty() ? "where" : "",
                whereExpression,
                // conjunct where expression from query with group by predicate
                (!whereExpression.isEmpty() && !groupByPredicatesString.isEmpty() ? "/\\" : ""),
                groupByPredicatesString);

        // Counting the length of an array or set in minizinc when used for constraints is done by summation
        // count([col1[h] | h in 1..X]) becomes sum([1 | h in 1..X]).
        if (innerOnly) {
            final String head = evaluateHeadItem(headItem, innerComprehensionQualifiers);
            return String.format("%s", head);
        }
        /*
         * At this point, we have the inner comprehension. Now we produce the outer comprehension, starting
         * with iterating over all groups. We use a helper function that returns the set of groups by the
         * number
         */
        final String groupIndexSet = String.format("1..GROUP_TABLE__%s__NUM_ROWS",
                                                   viewName.toUpperCase(Locale.getDefault()));
        /*
         * We go over all having clauses, and produce the having string. This becomes the where clause
         * for the outer comprehension.
         */
        final List<String> havingClauses = new ArrayList<>();
        for (final BinaryOperatorPredicateWithAggregate predicate : aggregateQualifiers) {
            final String havingClause = evaluateHavingClause(predicate);
            havingClauses.add(havingClause);
        }
        final String havingClauseString = String.join(" /\\ ", havingClauses);
        final String head = evaluateHeadItem(headItem, innerComprehensionQualifiers);
        return String.format("[%s | %s in %s %s]", head, GROUP_KEY, groupIndexSet,
                havingClauses.isEmpty() ? "" : "where " + havingClauseString);
    }

    private String evaluateHeadItem(final Expr headItem) {
        return evaluateHeadItem(headItem, null);
    }

    /**
     * Converts a list comprehension head item into a string. This call can also be used to
     * evaluate a head item as part of a group by. To
     * do so, the caller needs to supply a String that represents the inner comprehension of a group
     * by.
     *
     * For example:
     *       select sum(T.A) from T group by T.B.
     *
     * ...becomes:
     *      [sum([T__A[i] | i in 1..T__NUM_ROWS, T__A[i] == T__A[GROUP_KEY]) | GROUP_KEY in {k | k in T.B}]
     *       \__________/   \_______________________________________________/
     *            |                                   |
     *            |                                   |
     *         headItem                groupByInnerComprehensionQualifier
     *       \______________________________________________________________/
     *                                      |
     *                                      |
     *                          Return value of this method
     *
     * @param headItem an expr representing a select item from an SQL query.
     * @param groupByInnerComprehensionQualifier if null, this is not an aggregate statement.
     * @return a String representation of an evaluated head item.
     */
    private String evaluateHeadItem(final Expr headItem, @Nullable final String groupByInnerComprehensionQualifier) {
        final ExpressionToStack visitor = new ExpressionToStack();
        visitor.visit(headItem);
        final ArrayDeque<Expr> stack = visitor.stack;
        final ArrayDeque<String> operands = new ArrayDeque<>();
        while (stack.size() != 0) {
            final Expr expr = stack.pop();
            if (expr instanceof FunctionCall) {
                final FunctionCall function = (FunctionCall) expr;
                final String functionName = FUNCTION_STRING_MAP.get(function.getFunction());
                Preconditions.checkArgument(function.getArgument().size() == 1);
                final Expr argument = function.getArgument().get(0);
                if (argument instanceof ColumnIdentifier) {
                    final ColumnIdentifier ci = (ColumnIdentifier) argument;
                    // Minizinc does not have a 'count' function. It therefore requires us to replace
                    // 'count' with 'sum', and the argument with '1'.
                    final String arg = MinizincString.columnNameWithIteration(ci);
                    final String op = String.format("%s([%s | %s])", functionName,
                            arg,
                            groupByInnerComprehensionQualifier);
                    operands.push(op);
                } else if (argument instanceof BinaryOperatorPredicate) {
                    final BinaryOperatorPredicate bop = (BinaryOperatorPredicate) argument;
                    final String arg = evaluateHeadItem(bop, null);
                    final String op = String.format("%s([%s | %s])", functionName,
                            arg,
                            groupByInnerComprehensionQualifier);
                    operands.push(op);
                } else if (argument instanceof Literal) {
                    final String op = String.format("%s([%s | %s])", functionName,
                            ((Literal) argument).getValue(),
                            groupByInnerComprehensionQualifier);
                    operands.push(op);
                } else {
                    // We're usually here because of unary operators like -(count(col1)) in select expressions.
                    Preconditions.checkArgument(function.getArgument().size() == 1);
                    final Expr arg = function.getArgument().get(0);
                    final String argumentAsString = evaluateHeadItem(arg, groupByInnerComprehensionQualifier);
                    final String op = String.format("%s(%s)", functionName, argumentAsString);
                    operands.push(op);
                }
            } else if (expr instanceof ColumnIdentifier) {
                // If this call is triggered for a group-by, then columns being selected are always
                // keyed by the GROUP_KEYS. For non-aggregate queries, the columns are keyed by their respective
                // table iterators.
                final String op = groupByInnerComprehensionQualifier == null ?
                        MinizincString.columnNameWithIteration((ColumnIdentifier) expr) :
                        MinizincString.groupColumnNameWithIteration(viewName, (ColumnIdentifier) expr);
                operands.push(op);
            } else if (expr instanceof Literal) {
                operands.push(((Literal) expr).getValue().toString());
            } else if (expr instanceof BinaryOperatorPredicate) {
                final String op1 = operands.pop();
                final String op2 = operands.pop();
                final BinaryOperatorPredicate operator = (BinaryOperatorPredicate) expr;
                operands.push(String.format("(%s) %s (%s)", op1, operatorToString(operator.getOperator()), op2));
            } else if (expr instanceof UnaryOperator) {
                final UnaryOperator unaryOperator = (UnaryOperator) expr;
                final String operator;
                switch (unaryOperator.getOperator()) {
                    case MINUS:
                        operator = "-";
                        break;
                    case NOT:
                        operator = "not";
                        break;
                    default:
                        throw new IllegalArgumentException(unaryOperator.toString());
                }
                final String exprStr = evaluateHeadItem(unaryOperator.getArgument(),
                                                        groupByInnerComprehensionQualifier);
                operands.push(String.format("%s(%s)", operator, exprStr));
            } else {
                throw new RuntimeException("Unexpected expr type: " + expr);
            }
        }
        assert operands.size() == 1;
        return operands.getFirst();
    }

    /**
     * @return a list of strings that represent an expression (like in a where clause)
     */
    private List<String> evaluateExpression() {
        final List<String> ret = new ArrayList<>();
        /*
         * Expressions without a 'head' are either literals or predicates. We return the
         * corresponding Minizinc form for each of them.
         */
        if (headItems.isEmpty()) {
            // The output is either a literal or a predicate expression, which we return as is.
            if (literals.size() == 1) {
                // This might be a predicate function
                if (literals.get(0) instanceof FunctionCall) {
                    final MinizincCodeGenerator cg = new MinizincCodeGenerator(viewName);
                    final FunctionCall function = (FunctionCall) literals.get(0);
                    Preconditions.checkArgument(function.getArgument().size() == 1);
                    cg.visit(function.getArgument().get(0));
                    return ImmutableList.of(String.format("%s(%s)", function.getFunction(),
                                                                             cg.evaluateExpression().get(0)));
                }
                else if (literals.get(0) instanceof UnaryOperator) {
                    final MinizincCodeGenerator cg = new MinizincCodeGenerator(viewName);
                    final UnaryOperator operator = (UnaryOperator) literals.get(0);
                    cg.visit(operator.getArgument());
                    final String opStr;
                    switch (operator.getOperator()) {
                        case NOT:
                            opStr = "not";
                            break;
                        case MINUS:
                            opStr = "-";
                            break;
                        default:
                            throw new IllegalArgumentException(operator.toString());
                    }
                    return ImmutableList.of(String.format("%s(%s)", opStr, cg.evaluateExpression().get(0)));
                }
                else if (literals.get(0) instanceof ExistsPredicate) {
                    final ExistsPredicate predicate = (ExistsPredicate) literals.get(0);
                    final MinizincCodeGenerator cg = new MinizincCodeGenerator(viewName);
                    cg.visit(predicate.getArgument());
                    return ImmutableList.copyOf(cg.generateConstraintViewCodeInner("exists"));
                }
                // This is a literal
                else {
                    final String body = MinizincString.literal(literals.get(0));
                    return ImmutableList.of(body);
                }
            } else if (whereQualifiers.size() == 1) {
                final String whereExpressionString = evaluateWhereExpression(whereQualifiers.get(0));
                return ImmutableList.of(whereExpressionString);
            } else if (aggregateQualifiers.size() == 1) {
                return ImmutableList.of(evaluateHavingClause(aggregateQualifiers.get(0)));
            } else {
                assert false;
            }
        } else {
            /*
             * Expressions with a head are list comprehensions. Minizinc does not support
             * multiple head items in a comprehension, so we produce one comprehension per head item.
             */
            for (final Expr expr : headItems) {
                final String expressionString = generatorExpressionFromSelectWhere(expr, false);
                ret.add(expressionString);
            }
        }
        return ImmutableList.copyOf(ret);
    }


    /**
     * Converts a single predicate in a having clause into a string.
     *
     * @param node a predicate within a having clause.
     * @return a string representation of the supplied predicate.
     */
    private String evaluateHavingClause(final BinaryOperatorPredicateWithAggregate node) {
        final ExpressionToStack visitor = new ExpressionToStack();
        visitor.visit(node);
        final ArrayDeque<Expr> stack = visitor.stack;
        final ArrayDeque<String> operands = new ArrayDeque<>();
        while (stack.size() != 0) {
            final Expr expr = stack.pop();
            if (expr instanceof FunctionCall) {
                final String s = evaluateOperandInHavingClausePredicate(expr);
                operands.push(s);
            } else if (expr instanceof BinaryOperatorPredicate) {
                final String left = operands.pop();
                final String right = operands.pop();
                final BinaryOperatorPredicate.Operator operator = ((BinaryOperatorPredicate) expr).getOperator();
                operands.push(String.format("(%s) %s (%s)", left, operatorToString(operator), right));
            } else if (expr instanceof ColumnIdentifier) {
                operands.push(MinizincString.groupColumnNameWithIteration(viewName, (ColumnIdentifier) expr));
            } else if (expr instanceof Literal) {
                operands.push(((Literal) expr).getValue().toString());
            } else {
                operands.push(evaluateOperandInHavingClausePredicate(expr));
            }
        }
        assert operands.size() == 1;
        return operands.getFirst();
    }

    /**
     * Evaluates an operand within a having clause predicate.
     *
     * @param operand an operand of a having clause predicate.
     * @return a string representation of the supplied operand.
     */
    private String evaluateOperandInHavingClausePredicate(final Expr operand) {
        if (operand instanceof FunctionCall) {
            return generatorExpressionFromSelectWhere(operand, true);
        } else {
            final MinizincCodeGenerator visitor = new MinizincCodeGenerator(viewName);
            visitor.visit(operand);
            return visitor.evaluateExpression().get(0);
        }
    }


    /**
     * We built individual checks for each pk field. For each one we make sure the field is unique within that table.
     * We only do this for primary keys that have at least one CONTROLLABLE field. At the end we just join all the
     * individual checks with an '/\' to form a single condition for the primary key
     *
     * @return Only returns a constraint if this table has a primary key
     * and at least one CONTROLLABLE field
     */
    private String getPrimaryKeyDeclaration(final IRPrimaryKey primaryKey) {
        if (!primaryKey.getPrimaryKeyFields().isEmpty() && primaryKey.hasControllableColumn()) {
            return String.format("constraint forall ( i,j in 1..%s where i < j ) ( not( %s ) );",
                    MinizincString.tableNumRowsName(primaryKey.getIRTable()),
                    // build minizinc condition for each single field
                    primaryKey.getPrimaryKeyFields().stream().map(field -> String.format("%s[i] == %s[j]",
                            MinizincString.qualifiedName(field),
                            MinizincString.qualifiedName(field)))
                            .collect(Collectors.joining(MinizincString.MNZ_AND))
            );
        }
        return "";
    }


    /**
     * For each foreign key we built individual checks for each field.
     * For each field we make sure the value of the field from the child table is within the values of the parent table.
     * We only do this for foreign keys that have at least one CONTROLLABLE field.
     * At the end we just join all the individual checks with an '/\' to form a single condition for each foreign key
     *
     * @return Returns the MiniZinc constraint declaration
     */
    private String getForeignKeyDeclaration(final IRForeignKey foreignKey) {
        if (foreignKey.hasConstraint()) {
            return String.format("constraint forall ( i in 1..%s ) ( %s );",
                    MinizincString.tableNumRowsName(foreignKey.getChildTable()),
                    foreignKey.getFields().entrySet().stream()
                            .map(e -> String.format(" %s[i] in { j | j in %s } ",
                                    MinizincString.qualifiedName(e.getKey()),
                                    MinizincString.qualifiedName(e.getValue())
                            ))
                            .collect(Collectors.joining(MinizincString.MNZ_AND))
            );
        }
        return "";
    }

    private static class ExpressionToStack extends SimpleVisitor {
        private final ArrayDeque<Expr> stack = new ArrayDeque<>();

        @Override
        protected VoidType visitFunctionCall(final FunctionCall node, final VoidType context) {
            stack.push(node);
            return defaultReturn();
        }

        @Override
        protected VoidType visitUnaryOperator(final UnaryOperator node, final VoidType context) {
            stack.push(node);
            return defaultReturn();
        }

        @Override
        protected VoidType visitColumnIdentifier(final ColumnIdentifier node, final VoidType context) {
            stack.push(node);
            return defaultReturn();
        }

        @Override
        protected VoidType visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final VoidType context) {
            stack.push(node);
            return super.visitBinaryOperatorPredicate(node, context);
        }

        @Override
        protected VoidType visitLiteral(final Literal node, final VoidType context) {
            stack.push(node);
            return defaultReturn();
        }

        @Override
        protected VoidType visitListComprehension(final ListComprehension node, final VoidType context) {
            stack.push(node);
            return defaultReturn();
        }
    }

    private VarType usesControllableVariables(final Expr expr) {
        if (expr instanceof GroupByComprehension) {
            final GroupByComprehension comprehension = (GroupByComprehension) expr;
            final List<ColumnIdentifier> columnIdentifiers = comprehension.getGroupByQualifier()
                                                                          .getGroupByColumnIdentifiers();
            for (final ColumnIdentifier columnIdentifier: columnIdentifiers) {
                if (columnIdentifier.getField().isControllable()) {
                    return VarType.IS_VAR;
                }
            }
            final ListComprehension inner = comprehension.getComprehension();
            for (final Qualifier qualifer: inner.getQualifiers()) {
                final UsesControllableFields visitor = new UsesControllableFields();
                visitor.visit(qualifer);
                if (visitor.usesControllableFields() && qualifer instanceof BinaryOperatorPredicate) {
                    return VarType.IS_VAR;
                }
            }
        }
        else if (expr instanceof ColumnIdentifier) {
            final UsesControllableFields visitor = new UsesControllableFields();
            visitor.visit(expr);
            if (visitor.usesControllableFields()) {
                return VarType.IS_VAR;
            }
        }
        else if (expr instanceof ListComprehension) {
            final ListComprehension comprehension = (ListComprehension) expr;
            for (final Qualifier qualifer: comprehension.getQualifiers()) {
                final UsesControllableFields visitor = new UsesControllableFields();
                visitor.visit(qualifer);
                if (visitor.usesControllableFields() && qualifer instanceof BinaryOperatorPredicate) {
                    return VarType.IS_OPT;
                }
            }
            /* TODO: need if comprehension is var because it depends on a var view, but it's not an opt
             */
        }
        else if (expr instanceof FunctionCall) {
            Preconditions.checkArgument(((FunctionCall) expr).getArgument().size() == 1);
            return usesControllableVariables(((FunctionCall) expr).getArgument().get(0));
        }
        else if (expr instanceof BinaryOperatorPredicate) {
            final BinaryOperatorPredicate predicate = (BinaryOperatorPredicate) expr;
            return getMax(usesControllableVariables(predicate.getLeft()),
                          usesControllableVariables(predicate.getRight()));
        }
        else if (expr instanceof Literal) {
            return VarType.IS_INT;
        }
        else {
            throw new RuntimeException("Unhandled case " + expr);
        }
        return VarType.IS_INT;
    }

    private enum VarType {
        IS_INT,
        IS_VAR,
        IS_OPT,
    }

    static {
        VAR_TYPE_STRING.put(VarType.IS_INT, "int");
        VAR_TYPE_STRING.put(VarType.IS_VAR, "var int");
        VAR_TYPE_STRING.put(VarType.IS_OPT, "var opt int");
    }

    private VarType getMax(final VarType v1, final VarType v2) {
        if (v1.compareTo(v2) > 0) {
            return v1;
        }
        return v2;
    }

    private String operatorToString(final BinaryOperatorPredicate.Operator operator) {
        switch (operator) {
            case EQUAL:
                return "==";
            case NOT_EQUAL:
                return "!=";
            case AND:
                return "/\\";
            case OR:
                return "\\/";
            case IN:
                return "in";
            case LESS_THAN_OR_EQUAL:
                return "<=";
            case LESS_THAN:
                return "<";
            case GREATER_THAN_OR_EQUAL:
                return ">=";
            case GREATER_THAN:
                return ">";
            case ADD:
                return "+";
            case SUBTRACT:
                return "-";
            case MULTIPLY:
                return "*";
            case DIVIDE:
                return "div";
            default:
                throw new UnsupportedOperationException("Operator " + operator);
        }
    }
}