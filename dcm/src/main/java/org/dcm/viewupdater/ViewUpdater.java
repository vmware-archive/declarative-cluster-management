package org.dcm.viewupdater;

import org.dcm.IRTable;
import org.jooq.DSLContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ViewUpdater {
    protected static final List<String> UPDATE_QUERIES = new ArrayList<>();
    protected static List<String> baseTables = new ArrayList<>();
    protected DSLContext dbCtx = null;
    protected static Map<String, IRTable> irTables = new HashMap<>();

    public void initialize(final Map<String, IRTable> irTables, final DSLContext dbCtx, final List<String> baseTables) {
        this.baseTables = baseTables;
        this.dbCtx = dbCtx;
        this.irTables = irTables;
    }

    public abstract void createDBTriggers();

    public void flushUpdates() {
        UPDATE_QUERIES.forEach(q -> {
            dbCtx.execute(q);
        });
        UPDATE_QUERIES.clear();
    }
}
