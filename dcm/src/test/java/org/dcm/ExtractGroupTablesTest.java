/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QuerySpecification;
import org.junit.Test;

import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExtractGroupTablesTest {

    @Test
    public void testNoControllables() {
        final String input = "create view v as " +
                "select * " +
                " from t1 join t2 on t1.a = t2.b " +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final QuerySpecification orig = (QuerySpecification) statement.getQuery().getQueryBody();
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification rewritten = (QuerySpecification) process.get().getQuery().getQueryBody();
        assertEquals(orig.getFrom().get(), rewritten.getFrom().get());
    }

    @Test
    public void testOneComparisonWithControllable() {
        final String input = "create view v as " +
                "select * " +
                " from t1 join t2 on t1.controllable__a = t2.b " +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        assertFalse(join.getCriteria().isPresent());
    }

    @Test
    public void testTwoComparisonsOneControllable() {
        final String input = "create view v as " +
                "select * " +
                " from t1 join t2 on t1.controllable__a = t2.b and t1.x = t2.y" +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        final JoinCriteria joinCriteria = join.getCriteria().get();
        final ComparisonExpression comparisonExpression = (ComparisonExpression) ((JoinOn) joinCriteria)
                                                                                  .getExpression();
        assertEquals("t1.x", comparisonExpression.getLeft().toString());
        assertEquals("t2.y", comparisonExpression.getRight().toString());
    }

    @Test
    public void testThreeComparisonsOneControllable() {
        final String input = "create view v as " +
                " select * " +
                " from t1 join t2 on t1.x = t2.y and t1.controllable__a = t2.b and t2.c != t1.y" +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        final JoinCriteria joinCriteria = join.getCriteria().get();
        final LogicalBinaryExpression comparisonExpression = (LogicalBinaryExpression) ((JoinOn) joinCriteria)
                                                                .getExpression();

        assertEquals("(t1.x = t2.y)", comparisonExpression.getLeft().toString());
        assertEquals("(t2.c <> t1.y)", comparisonExpression.getRight().toString());
    }


    @Test
    public void testThreeComparisonsOneControllableWithOr() {
        final String input = "create view v as " +
                " select * " +
                " from t1 join t2 on t1.x = t2.y OR (t1.controllable__a = t2.b and t2.c != t1.y)" +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        final JoinCriteria joinCriteria = join.getCriteria().get();
        final LogicalBinaryExpression comparisonExpression = (LogicalBinaryExpression) ((JoinOn) joinCriteria)
                                                        .getExpression();
        assertEquals(LogicalBinaryExpression.Operator.OR, comparisonExpression.getOperator());
        assertEquals("(t1.x = t2.y)", comparisonExpression.getLeft().toString());
        assertEquals("(t2.c <> t1.y)", comparisonExpression.getRight().toString());
    }

    @Test
    public void testThreeComparisonsOneControllableWithRemovableOr() {
        final String input = "create view v as " +
                " select * " +
                " from t1 join t2 on t1.x = t2.y AND (t1.controllable__a = t2.b OR t2.c != t1.y)" +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        final JoinCriteria joinCriteria = join.getCriteria().get();
        final ComparisonExpression comparisonExpression = (ComparisonExpression) ((JoinOn) joinCriteria)
                .getExpression();
        assertEquals(ComparisonExpression.Operator.EQUAL, comparisonExpression.getOperator());
        assertEquals("t1.x", comparisonExpression.getLeft().toString());
        assertEquals("t2.y", comparisonExpression.getRight().toString());
    }

    @Test
    public void testThreeComparisonsOneControllableWithTwoOrs() {
        final String input = "create view v as " +
                " select * " +
                " from t1 join t2 on t1.x = t2.y OR t1.controllable__a = t2.b OR t2.c != t1.y" +
                " group by t2.b";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        assertFalse(join.getCriteria().isPresent());
    }
}
