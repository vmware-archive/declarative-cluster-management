package com.vrg;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.QuerySpecification;
import org.junit.Test;

import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExtractGroupTablesTest {

    @Test
    public void testNoControllables() {
        final String input = "create view least_requested_sums as\n" +
                "select sum(pod_info.cpu_request) as cpu_load\n" +
                "       from pod_info join node_info on pod_info.node_name = node_info.name " +
                " group by node_info.name";
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
        final String input = "create view least_requested_sums as\n" +
                "select sum(pod_info.cpu_request) as cpu_load\n" +
                "       from pod_info join node_info on pod_info.controllable__node_name = node_info.name " +
                " group by node_info.name";
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
    public void testMultipleComparisonsOneControllable() {
        final String input = "create view least_requested_sums as\n" +
                "select sum(pod_info.cpu_request) as cpu_load\n" +
                "       from pod_info join node_info on pod_info.controllable__node_name = node_info.name " +
                "                                   and pod_info.x = node_info.y" +
                " group by node_info.name";
        final SqlParser parser = new SqlParser();
        final CreateView statement = (CreateView) parser.createStatement(input, new ParsingOptions());
        final ExtractGroupTable visitor = new ExtractGroupTable();
        final Optional<CreateView> process = visitor.process(statement);
        assertTrue(process.isPresent());
        final QuerySpecification queryBody = (QuerySpecification) process.get().getQuery().getQueryBody();
        final Join join = (Join) queryBody.getFrom().get();
        final JoinCriteria joinCriteria = join.getCriteria().get();
        final ComparisonExpression comparisonExpression = (ComparisonExpression) (((JoinOn) joinCriteria)
                                                                                  .getExpression());
        assertEquals(comparisonExpression.getLeft().toString(), "pod_info.x");
        assertEquals(comparisonExpression.getRight().toString(), "node_info.y");
    }
}
