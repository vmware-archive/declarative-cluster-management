package org.dcm.viewupdater;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.dcm.IRTable;
import org.h2.api.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class H2Updater extends ViewUpdater {

    public H2Updater(final String modelName, final Connection connection,
                     final DSLContext dbCtx, final Map<String, IRTable> irTables,
                     final List<String> baseTables) {
        super(modelName, connection, dbCtx, baseTables, irTables);
        triggerClassName = H2Updater.InnerH2Updater.class.getName();
        createDBTriggers();
    }

    public static class InnerH2Updater implements Trigger {
        private String tableName;
        private String modelName;

        public InnerH2Updater() {

        }

        @Override
        public void init(final Connection connection, final String schemaName, final String triggerName,
                         final String tableName, final boolean before, final int type) throws SQLException {
            this.tableName = tableName;
            this.modelName = Iterables.get(Splitter.on('_').split(triggerName), 0);
        }

        @Override
        public void fire(final Connection connection, final Object[] oldRow,
                         final Object[] newRow) throws SQLException {
            mapRecordsFromDB.computeIfAbsent(modelName, m -> new ArrayList<>());
            mapRecordsFromDB.get(modelName).add(LocalDDlogCommand.newLocalDDlogCommand(tableName, newRow));
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public void remove() throws SQLException {

        }
    }
}
