package com.vmware.dcm;


import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.compiler.IRColumn.FieldType;
import com.vmware.dcm.generated.parser.DcmSqlParserImpl;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class AutoScope {
    private static final Logger LOG = LoggerFactory.getLogger(AutoScope.class);
    // Base table/view to apply scoping optimizations to
    private static final String BASE_TABLE = "spare_capacity_per_node";
    private static final String[] RESOURCE_TYPE = new String[]{"cpu", "memory", "pods", "ephemeral-storage"};
    private static final String BASE_COL = "name";
    private static final String VIEW_TEMPLATE = """
            SELECT DISTINCT name, resource, capacity FROM %s
            """;

    public static Map<String, String> augmentedViews(final List<String> constraints,
             final IRContext irContext, final int limit) {

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

        final Map<String, String> views = domainRestrictingViews(selectClause, irContext, limit);
        return views;
    }

    /**
     * Create view statements for views with specificied suffix.
     * Needed since _sort views might need to be created before _augment views.
     */
    public static List<String> getSuffixViewStatements(final Map<String, String> views, final String suffix) {
        final List<String> statements = new ArrayList<>();
        for (final var entry : views.entrySet()) {
            final String name = entry.getKey();
            final String statement = entry.getValue();
            if (name.contains(suffix.toUpperCase())) {
                statements.add(String.format("%nCREATE VIEW %s AS (%s)%n", name, statement));
            }
        }
        return statements;
    }

    /**
     * Create view statements based on view name and queries.
     */
    public static List<String> getViewStatements(final Map<String, String> views) {
        final List<String> augViews = views.entrySet().stream()
                .map(e -> String.format("%nCREATE VIEW %s AS %s%n", e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return augViews;
    }

    /**
     * Heuristic: Sort spare_capacity_per_node by decreasing resources
     * @param limit number of candidate nodes to fetch for each resource
     * @return SQL query that selects top K nodes for each resource
     */
    private static String customSort(final int limit) {
        final String template = "" +
                "(SELECT distinct name FROM %s WHERE resource = '%s'" +
                //" ORDER BY capacity DESC " +
                ")" +
                //LIMIT %d)
                "";
        final List<String> queries = new ArrayList<>();
        for (final String resource : RESOURCE_TYPE) {
            queries.add(String.format(template, BASE_TABLE, resource)); //, limit));
        }
        return String.join(" UNION ", queries);
    }

    /**
     * Generate domain restricting views for constraints in the form of [variable IN (SELECT clause)]
     * @param selectClause key: variable name, value: list of tableNames and fieldNames for filtering
     * @param irContext needed to check type of column (array versus values)
     * @param limit number of candidate nodes to keep per resource type (top K)
     * @return
     */
    private static Map<String, String> domainRestrictingViews(final Map<String, Map<String, String>> selectClause,
                           final IRContext irContext, final int limit) {
        final Map<String, String> views = new HashMap<>();
        // Create a custom view that sorts and truncates the base table
        final String sortView = customSort(limit);
        views.put((BASE_TABLE + DBViews.SORT_VIEW_NAME_SUFFIX).toUpperCase(), sortView);

        // Union of domain restricted queries
        for (final var e : selectClause.entrySet()) {
            final String var = e.getKey();
            final Map<String, String> clause = e.getValue();
            final List<String> queries = new ArrayList<>();
            for (final Map.Entry<String, String> entry : clause.entrySet()) {
                final String tableName = entry.getKey();
                final String fieldName = entry.getValue();
                // Check if table name is in IRContext
                if (!irContext.containTable(tableName)) {
                    continue;
                }
                if (!tableName.toLowerCase().equals(BASE_TABLE)) {
                    // IN field is an array: flatten array
                    if (irContext.getColumn(tableName, fieldName).getType() == FieldType.ARRAY) {
                        queries.add(String.format("(SELECT DISTINCT name,resource,capacity FROM" +
                                        " %s JOIN %s ON ARRAY_CONTAINS(%s.%s, %s.%s))",
                                BASE_TABLE, tableName.toLowerCase(Locale.ROOT), tableName.toLowerCase(Locale.ROOT), fieldName.toLowerCase(Locale.ROOT), BASE_TABLE, BASE_COL));
                    } else {
                        queries.add(String.format(
                                "(SELECT DISTINCT %s.name as name, %s.resource as resource ,%s.capacity as capacity " +
                                        "FROM %s " +
                                        " JOIN %s ON %s.%s = %s.%s)",
                                BASE_TABLE, BASE_TABLE, BASE_TABLE,
                                BASE_TABLE, tableName, BASE_TABLE, BASE_COL, tableName, fieldName));
                    }
                } else { // sort and truncate base table
                    final String view = BASE_TABLE + DBViews.SORT_VIEW_NAME_SUFFIX;
                    queries.add(String.format(
                            "(SELECT DISTINCT %s.name as name, %s.resource as resource ,%s.capacity as capacity " +
                                    "FROM %s " +
                            " JOIN %s ON %s.%s = %s.%s)",
                            BASE_TABLE, BASE_TABLE, BASE_TABLE,
                            BASE_TABLE, view, BASE_TABLE, BASE_COL, view, BASE_COL));
                }
            }
            final String query = String.join(" UNION ", queries);
            views.put((BASE_TABLE + DBViews.SCOPE_VIEW_NAME_SUFFIX).toUpperCase(), query);
        }
        return views;
    }


}
