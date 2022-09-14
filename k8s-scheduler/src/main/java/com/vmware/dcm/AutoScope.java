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
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;


class ScopeUtils {
    private final double DISCOUNT = 0.9;
    private final String tableName;
    private final String idField;
    private final String groupField;
    private final String sortField;
    private final RecordComparator rComparator = new RecordComparator();

    class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(final Record r1, final Record r2) {
            final String s1 = r1.toString();
            final String s2 = r2.toString();
            final int c = Long.compare(getSortVal(r2), getSortVal(r1));
            if (c == 0) {
                return s1.compareTo(s2);
            } else {
                return c;
            }
        }
    }

    ScopeUtils(final String tableName, final String idField, final String groupField, final String sortField) {
        this.tableName = tableName.toUpperCase();
        this.idField = idField.toUpperCase();
        this.groupField = groupField.toUpperCase();
        this.sortField = sortField.toUpperCase();
    }

    ScopeUtils(final String tableName, final String idField, final String groupField) {
        this.tableName = tableName.toUpperCase();
        this.idField = idField.toUpperCase();
        this.groupField = groupField.toUpperCase();
        this.sortField = "";
    }

    public boolean isSort() {
        return this.sortField.length() > 0;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isTargetTable(final Record r) {
        final String field = r.field(0).toString().toUpperCase();
        return field.contains(this.tableName);
    }

    private long getSortVal(final Record r) {
        return r.get(this.sortField, Long.class);
    }

    public RecordComparator getComparator() {
        return rComparator;
    }

    public String getGroup(final Record r) {
        return r.get(this.groupField).toString();
    }

    public String getID(final Record r) {
        return r.get(this.idField).toString();
    }

    public List<String> getIDs(final List<Record> records) {
        return records.stream().map(record -> getID(record)).collect(Collectors.toList());
    }

    public List<String> getTopkID(final ConcurrentSkipListSet<Record> records, final int k,
                                  final Map<String, Integer> softConstraints) {
        final Iterator<Record> iter = records.iterator();
        final int limit = Math.min(k, records.size());
        final Map<String, Double> discounted = new HashMap<>();
        final ArrayList<String> ids = new ArrayList<>(limit);
        for (int i = 0; i < limit; i ++) {
            final Record r = iter.next();
            final String id = getID(r);
            // Resource needs to be discounted due to soft constraints
            if (softConstraints.containsKey(id)) {
                Double val = new Double(getSortVal(r));
                val *= Math.pow(DISCOUNT, softConstraints.get(id));
                discounted.put(id, val);
            } else {
                ids.add(id);
            }
        }

        // Sort discounted nodes by resources
        final List<Map.Entry<String, Double>> sorted =
                discounted.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toList());

        int i = 0;
        while (iter.hasNext() && ids.size() < limit) {
            final Record r = iter.next();
            final double v1 = getSortVal(r);
            int j = i;
            while (j < sorted.size() && ids.size() < limit && sorted.get(j).getValue() >= v1) {
                ids.add(sorted.get(j).getKey());
                j += 1;
            }
            i = j;
            if (ids.size() < limit) {
                ids.add(getID(r));
            }
        }
        return ids;
    }
}


class DeltaProcessor implements DeltaCallBack {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaProcessor.class);
    private static final String GROUP_ID = "POD_UID";

    private final int limit;
    private List<ScopeUtils> utils;
    private Map<String, ConcurrentSkipListSet<Record>> topk;
    private HashMap<String, Set<Record>> idLookup;
    private Map<String, Map<String, ConcurrentSkipListSet<String>>> exclude;
    private Map<String, Integer> softConstraints;


    public DeltaProcessor(final String tableName, final String idField, final String groupField,
                        final String sortField, final int limit) {
        this.limit = limit;
        this.topk = new HashMap<>();
        this.exclude = new HashMap<>();
        this.utils = new ArrayList<>();
        this.utils.add(new ScopeUtils(tableName, idField, groupField, sortField));
        this.idLookup = new HashMap<>();
        this.softConstraints = new HashMap<>();
    }

    public void registerExclude(final String table, final String id) {
        this.exclude.put(table.toUpperCase(), new HashMap<>());
        this.utils.add(new ScopeUtils(table, id, GROUP_ID));
    }

    public Set<String> getExcludeIDs(final Map<String, ConcurrentSkipListSet<String>> map,
                                     final boolean intersect) {
        final Set<String> ids = new HashSet<>();
        boolean init = true;
        for (final Map.Entry<String, ConcurrentSkipListSet<String>> e : map.entrySet()) {
            if (init || !intersect) {
                ids.addAll(e.getValue());
                init = false;
            } else {
                ids.retainAll(e.getValue());
            }
        }
        return ids;
    }

    public void updateSoftConstraints() {
        softConstraints.clear();
        for (final Map.Entry<String, Map<String, ConcurrentSkipListSet<String>>> tbl : exclude.entrySet()) {
            final Map<String, ConcurrentSkipListSet<String>> map = tbl.getValue();
            for (final Map.Entry<String, ConcurrentSkipListSet<String>> e : map.entrySet()) {
                for (final String id : e.getValue()) {
                    softConstraints.put(id, softConstraints.getOrDefault(id, 0) + 1);
                }
            }
        }
    }


    public List<Record> getTopk() {
        final ScopeUtils sortHelper = utils.get(0);
        final Set<String> ids = new HashSet<>();
        // Discount values with soft constraints
        updateSoftConstraints();

        // Top k per resource
        for (final Map.Entry<String, ConcurrentSkipListSet<Record>> entry: topk.entrySet()) {
            final ConcurrentSkipListSet<Record> records = entry.getValue();
            ids.addAll(sortHelper.getTopkID(records, limit, softConstraints));
        }
        // Apply exclusion
        for (final Map.Entry<String, Map<String, ConcurrentSkipListSet<String>>> tbl : exclude.entrySet()) {
            final Set<String> toRemove = getExcludeIDs(tbl.getValue(), true);
            ids.removeAll(toRemove);
            //LOG.info("EXCLUDE!!!!!!! " + toRemove.size());
        }


        final List<Record> results = new ArrayList<>();
        for (final String id : ids) {
            results.addAll(idLookup.get(id));
        }
        return results;
    }

    private void updateTopk(final DeltaType type, final Record r, final ScopeUtils u) {
        final String group = u.getGroup(r);
        final String id = u.getID(r);
        if (type == DeltaCallBack.DeltaType.ADD) {
            final ConcurrentSkipListSet<Record> r1 = topk.getOrDefault(
                    group, new ConcurrentSkipListSet<>(u.getComparator()));
            r1.add(r);
            topk.put(group, r1);

            final Set<Record> r2 = idLookup.getOrDefault(id, new HashSet<>());
            r2.add(r);
            idLookup.put(id, r2);
        } else {
            topk.get(group).remove(r);
            idLookup.get(id).remove(r);
        }
    }

    private void updateExclude(final DeltaType type, final Record r, final ScopeUtils u) {
        final String group = u.getGroup(r);
        final String id = u.getID(r);
        final String table = u.getTableName();
        final Map<String, ConcurrentSkipListSet<String>> map = exclude.get(table);
        if (type == DeltaCallBack.DeltaType.ADD) {
            final ConcurrentSkipListSet<String> ids = map.getOrDefault(group, new ConcurrentSkipListSet<String>());
            ids.add(id);
            map.put(group, ids);
        } else {
            map.get(group).remove(id);
        }
    }

    @Override
    public void processDelta(final DeltaType type, final Record r) {
        for (final ScopeUtils u : utils) {
            if (u.isTargetTable(r)) {
                if (u.isSort()) {
                    updateTopk(type, r, u);
                } else {
                    updateExclude(type, r, u);
                }
            }
        }

    }
}


public class AutoScope {
    private static final Logger LOG = LoggerFactory.getLogger(AutoScope.class);

    private final int limit;
    private DeltaProcessor delta;

    // Base table/view to apply scoping optimizations to
    private static final String BASE_TABLE = "spare_capacity_per_node";
    private static final String BASE_COL = "name";
    private static final String GROUP_COL = "resource";
    private static final String SORT_COL = "capacity";

    public AutoScope(final int limit) {
        this.limit = limit;
        this.delta = new DeltaProcessor(BASE_TABLE, BASE_COL, GROUP_COL, SORT_COL, limit);
    }

    public DeltaProcessor getCallBack() {
        return delta;
    }

    public List<Record> getSortView() {
        return delta.getTopk();
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

        final Map<String, Map<String, String>> inClause = new HashMap<>();
        final Map<String, Map<String, String>> notInClause = new HashMap<>();
        final Map<String, List<String>> consts = new HashMap<>();
        final ExtractConstraintInQuery visitor = new ExtractConstraintInQuery(
                inClause, notInClause, consts);
        for (final SqlCreateConstraint view : constraintViews) {
            visitor.visit(view);
        }

        // Register exclusion tables to the callback processing
        for (final Map.Entry<String, Map<String, String>> var: notInClause.entrySet()) {
            final Map<String, String> v = var.getValue();
            for (final Map.Entry<String, String> e : v.entrySet()) {
                this.delta.registerExclude(e.getKey(), e.getValue());
            }
        }
        final Map<String, String> views = domainRestrictingViews(inClause, consts, irContext);
        return views;
    }

    /**
     * Create view statements for views with specified suffix.
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

    private List<String> genDRQ(final Map<String, String> clause,
                                final IRContext irContext) {
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
                            BASE_TABLE, tableName  .toLowerCase(), tableName.toLowerCase(),
                            fieldName.toLowerCase(), BASE_TABLE, BASE_COL));
                } else {
                    queries.add(String.format(
                            "(SELECT DISTINCT %s.name as name, %s.resource as resource ,%s.capacity as capacity " +
                                    "FROM %s " +
                                    " JOIN %s ON %s.%s = %s.%s)",
                            BASE_TABLE, BASE_TABLE, BASE_TABLE,
                            BASE_TABLE, tableName.toLowerCase(), BASE_TABLE, BASE_COL,
                            tableName.toLowerCase(), fieldName.toLowerCase()));
                }
            }
        }
        return queries;
    }

    /**
     * Generate domain restricting views for constraints in the form of [variable IN (SELECT clause)]
     * @param inClause key: variable name, value: list of tableNames and fieldNames for filtering
     * @param irContext needed to check type of column (array versus values)
     * @return
     */
    private Map<String, String> domainRestrictingViews(final Map<String, Map<String, String>> inClause,
           final Map<String, List<String>> consts, final IRContext irContext) {
        final Map<String, String> views = new HashMap<>();

        // Union of domain restricted queries
        final Set<String> vars = new HashSet<>();
        vars.addAll(inClause.keySet());
        vars.addAll(consts.keySet());
        for (final String var : vars) {
            final List<String> queries = genDRQ(inClause.get(var), irContext);
            final String include = String.join(" UNION ", queries);
            views.put((BASE_TABLE + DBViews.INCLUDE_VIEW_NAME_SUFFIX).toUpperCase(), include);
        }
        return views;
    }


}
