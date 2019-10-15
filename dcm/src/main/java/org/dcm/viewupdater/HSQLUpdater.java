package org.dcm.viewupdater;

import org.dcm.IRTable;
import org.hsqldb.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;

import java.util.List;
import java.util.Map;

public class HSQLUpdater extends ViewUpdater {

    public HSQLUpdater(final Connection connection, final DSLContext dbCtx, final Map<String, IRTable> irTables,
                       final List<String> baseTables) {
        super(connection, dbCtx, baseTables, irTables);
        triggerClassName = HSQLUpdater.InnerHSQLUpdater.class.getName();
        createDBTriggers();
    }

    public static class InnerHSQLUpdater implements Trigger {

        public InnerHSQLUpdater() {

        }

        @Override
        public void fire(final int type, final String trigName,
                         final String tabName, final Object[] oldRow, final Object[] newRow) {
            recordsFromDB.add(LocalDDlogCommand.newLocalDDlogCommand(tabName, newRow));
        }
    }
}
