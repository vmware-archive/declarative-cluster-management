package com.vmware.dcm;


import com.vmware.dcm.generated.parser.DcmSqlParserImpl;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;


public class AutoScope {
    private static final Logger LOG = LoggerFactory.getLogger(AutoScope.class);
    // Base table/view to apply scoping optimizations to
    private static final String BASE_TABLE = "spare_capacity_per_node";
    private static final String BASE_COL = "name";
    private static final String VIEW_TEMPLATE = """
            SELECT DISTINCT name,resource,capacity FROM (%s)
            """;

    public static Map<String, String> augmentedViews(final List<String> schema, final List<String> constraints) {
        final List<SqlCreateConstraint> constraintViews = constraints.stream().map(
                constraint -> {
                    try {
                        final SqlParser.Config config = SqlParser.config()
                                .withParserFactory(DcmSqlParserImpl.FACTORY)
                                .withConformance(SqlConformanceEnum.LENIENT);
                        final SqlParser parser = SqlParser.create(constraint, config);
                        return (SqlCreateConstraint) parser.parseStmt();
                    } catch (final SqlParseException e) {
                        LOG.error("Could not parse view: {}", constraint, e);
                        throw new ModelException(e);
                    }
                }
        ).collect(Collectors.toList());

        final Map<String, Map<String, String>> selectClause = new HashMap<>();
        final ExtractConstraintInQuery visitor = new ExtractConstraintInQuery(selectClause);
        for (final SqlCreateConstraint view : constraintViews) {
            visitor.visit(view);
        }

        final Map<String, String> views = genView(selectClause);
        return views;
    }

    public static List<String> getViewStatements(final Map<String, String> views) {
        final List<String> augViews = views.entrySet().stream()
                .map(e -> String.format("%nCREATE VIEW %s AS %s%n", e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return augViews;
    }

    private static Map<String, String> genView(final Map<String, Map<String, String>> selectClause) {
        final Map<String, String> views = new HashMap<>();
        for (final String var : selectClause.keySet()) {
            final Map<String, String> clause = selectClause.get(var);
            final List<String> queries = new ArrayList<>();
            for (final Map.Entry<String, String> entry : clause.entrySet()) {
                if (!entry.getKey().toLowerCase().equals(BASE_TABLE)) {
                    queries.add(String.format("SELECT DISTINCT name,resource,capacity FROM" +
                            "(%s JOIN %s ON ARRAY_CONTAINS(%s.%s, %s.%s))",
                            BASE_TABLE, entry.getKey(), entry.getKey(), entry.getValue(), BASE_TABLE, BASE_COL));
                }
            }
            final String query = String.join(" UNION ", queries);
            final String view = String.format(VIEW_TEMPLATE, query);
            views.put((BASE_TABLE + DBViews.SCOPE_VIEW_NAME_SUFFIX).toUpperCase(), view);
        }
        return views;
    }


}
