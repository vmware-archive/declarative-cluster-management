package com.vmware.dcm;


import com.vmware.dcm.compiler.IRColumn.FieldType;
import com.vmware.dcm.compiler.IRContext;
import com.vmware.dcm.generated.parser.DcmSqlParserImpl;
import com.vmware.dcm.parser.SqlCreateConstraint;
import com.vmware.ddlog.util.DeltaCallBack;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;


class SortHelper {
    private final String tableName;
    private final String groupField;
    private final String sortField;
    private final RecordComparator rComparator = new RecordComparator();

    class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(final Record r1, final Record r2) {
            return Long.compare(getSortVal(r2), getSortVal(r1));
        }
    }

    SortHelper(final String tableName, final String groupField, final String sortField) {
        this.tableName = tableName.toUpperCase();
        this.groupField = groupField.toUpperCase();
        this.sortField = sortField.toUpperCase();
    }

    public boolean isSortTable(final Record r) {
        final String field = r.field(0).toString().toUpperCase();
        return field.contains(this.tableName);
    }

    private long getSortVal(final Record r) {
        return r.get(this.sortField, Long.class);
    }

    public String getGroup(final Record r) {
        return r.get(this.groupField).toString();
    }

    public List<Record> getTopk(final Set<Record> records, final int k) {
        final List<Record> sorted = new ArrayList<>(records);
        if (records.size() <= k) {
            return sorted;
        }

        Collections.sort(sorted, rComparator);
        return sorted.subList(0, k);
    }
}

class TopkPerGroup implements DeltaCallBack {
    private static final Logger LOG = LoggerFactory.getLogger(TopkPerGroup.class);

    private final int limit;
    private SortHelper helper;
    private Map<String, Set<Record>> topk;

    public TopkPerGroup(final String tableName, final String groupField, final String sortField, final int limit) {
        this.limit = limit;
        this.topk = new HashMap<>();
        this.helper = new SortHelper(tableName, groupField, sortField);
    }

    public List<Record> getTopk() {
        final List<Record> results = new ArrayList<>();
        for (final Map.Entry<String, Set<Record>> entry: topk.entrySet()) {
            final Set<Record> records = entry.getValue();
            results.addAll(helper.getTopk(records, limit));
        }
        return results;
    }

    @Override
    public void processDelta(final DeltaType type, final Record r) {
        if (helper.isSortTable(r)) {
            final String group = helper.getGroup(r);
            if (type == DeltaCallBack.DeltaType.ADD) {
                final Set<Record> records = topk.getOrDefault(group, new HashSet<>());
                records.add(r);
                topk.put(group, records);
            } else {
                topk.get(group).remove(r);
            }
        }
    }
}


public class AutoScope {
    private static final Logger LOG = LoggerFactory.getLogger(AutoScope.class);

    private final int limit;
    private TopkPerGroup topk;

    // Base table/view to apply scoping optimizations to
    private static final String BASE_TABLE = "spare_capacity_per_node";
    private static final String BASE_COL = "name";
    private static final String GROUP_COL = "resource";
    private static final String SORT_COL = "capacity";



    public AutoScope(final int limit) {
        this.limit = limit;
        this.topk = new TopkPerGroup(BASE_TABLE, GROUP_COL, SORT_COL, limit);
    }

    public TopkPerGroup getCallBack() {
        return topk;
    }

    public List<Record> getSortView() {
        return topk.getTopk();
    }

    public Map<String, String> augmentedViews(final List<String> constraints,
             final IRContext irContext) {

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

        final Map<String, String> views = domainRestrictingViews(selectClause, irContext);
        return views;
    }

    /**
     * Create view statements for views with specified suffix.
     * Needed since _sort views might need to be created before _augment views.
     */
    public List<String> getSuffixViewStatements(final Map<String, String> views, final String suffix) {
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
    public List<String> getViewStatements(final Map<String, String> views) {
        final List<String> augViews = views.entrySet().stream()
                .map(e -> String.format("%nCREATE VIEW %s AS %s%n", e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return augViews;
    }

    /**
     * Generate domain restricting views for constraints in the form of [variable IN (SELECT clause)]
     * @param selectClause key: variable name, value: list of tableNames and fieldNames for filtering
     * @param irContext needed to check type of column (array versus values)
     * @return
     */
    private Map<String, String> domainRestrictingViews(final Map<String, Map<String, String>> selectClause,
                           final IRContext irContext) {
        final Map<String, String> views = new HashMap<>();

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
                                    BASE_TABLE, tableName.toLowerCase(), tableName.toLowerCase(),
                                    fieldName.toLowerCase(), BASE_TABLE, BASE_COL));
                    } else {
                        queries.add(String.format(
                                "(SELECT DISTINCT %s.name as name, %s.resource as resource ,%s.capacity as capacity " +
                                        "FROM %s " +
                                        " JOIN %s ON %s.%s = %s.%s)",
                                BASE_TABLE, BASE_TABLE, BASE_TABLE,
                                BASE_TABLE, tableName, BASE_TABLE, BASE_COL, tableName, fieldName));
                    }
                }
            }
            final String query = String.join(" UNION ", queries);
            views.put((BASE_TABLE + DBViews.SCOPE_VIEW_NAME_SUFFIX).toUpperCase(), query);
        }
        return views;
    }


}
