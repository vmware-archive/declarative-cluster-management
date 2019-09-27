package org.dcm.viewupdater;

import org.dcm.IRTable;
import org.jooq.DSLContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ViewUpdater {
    static final List<String> UPDATE_QUERIES = new ArrayList<>();
    static List<String> baseTables = new ArrayList<>();
    protected DSLContext dbCtx = null;
    static Map<String, IRTable> irTables = new HashMap<>();

    protected static final String INTEGER_TYPE = "java.lang.Integer";
    protected static final String STRING_TYPE = "java.lang.String";
    protected static final String BOOLEAN_TYPE = "java.lang.Boolean";
    protected static final String LONG_TYPE = "java.lang.Long";

    public ViewUpdater() {

    }

    public ViewUpdater(final Map<String, IRTable> irTables, final DSLContext dbCtx, final List<String> baseTables) {
        ViewUpdater.baseTables = baseTables;
        this.dbCtx = dbCtx;
        ViewUpdater.irTables = irTables;
    }

    public abstract void createDBTriggers();

    public void flushUpdates() {
        UPDATE_QUERIES.forEach(q -> {
            System.out.println(q);
            dbCtx.execute(q);
        });
        UPDATE_QUERIES.clear();
    }
}
