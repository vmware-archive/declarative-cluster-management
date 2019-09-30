package org.dcm.viewupdater;

import org.dcm.IRTable;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ViewUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(ViewUpdater.class);

    final List<String> baseTables;
    protected final DSLContext dbCtx;

    static final List<String> UPDATE_QUERIES = new ArrayList<>();
    static Map<String, IRTable> irTables = new HashMap<>();

    protected static final String INTEGER_TYPE = "java.lang.Integer";
    protected static final String STRING_TYPE = "java.lang.String";
    protected static final String BOOLEAN_TYPE = "java.lang.Boolean";
    protected static final String LONG_TYPE = "java.lang.Long";

    public ViewUpdater(final DSLContext dbCtx, final List<String> baseTables) {
        this.baseTables = baseTables;
        this.dbCtx = dbCtx;
    }

    public static void setIRTables(final Map<String, IRTable> irTables) {
        ViewUpdater.irTables = irTables;
    }

    public abstract void createDBTriggers();

    public void flushUpdates() {
        UPDATE_QUERIES.forEach(query -> {
            LOG.info("Query: " + query);
            dbCtx.execute(query);
        });
        UPDATE_QUERIES.clear();
    }
}
