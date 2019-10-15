package org.dcm.viewupdater;

import org.dcm.IRTable;
import org.h2.api.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class H2Updater extends ViewUpdater {

    public H2Updater(final Connection connection, final DSLContext dbCtx, final Map<String, IRTable> irTables,
                       final List<String> baseTables) {
        super(connection, dbCtx, baseTables, irTables);
        triggerClassName = H2Updater.InnerH2Updater.class.getName();
        createDBTriggers();
    }

    public static class InnerH2Updater implements Trigger {
        private String tableName;

        public InnerH2Updater() {

        }

        @Override
        public void init(final Connection connection, final String schemaName, final String triggerName,
                         final String tableName, final boolean before, final int type) throws SQLException {
            this.tableName = tableName;
        }

        @Override
        public void fire(final Connection connection, final Object[] oldRow,
                         final Object[] newRow) throws SQLException {
            recordsFromDB.add(LocalDDlogCommand.newLocalDDlogCommand(tableName, newRow));
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public void remove() throws SQLException {

        }
    }
}
