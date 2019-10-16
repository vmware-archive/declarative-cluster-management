package org.dcm.viewupdater;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.dcm.IRTable;
import org.hsqldb.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HSQLUpdater extends ViewUpdater {

    public HSQLUpdater(final String modelName, final Connection connection,
                       final DSLContext dbCtx, final Map<String, IRTable> irTables,
                       final List<String> baseTables) {
        super(modelName, connection, dbCtx, baseTables, irTables);
        triggerClassName = HSQLUpdater.InnerHSQLUpdater.class.getName();
        createDBTriggers();
    }

    public static class InnerHSQLUpdater implements Trigger {
        private String modelName;

        public InnerHSQLUpdater() {

        }

        @Override
        public void fire(final int type, final String triggerName,
                         final String tableName, final Object[] oldRow, final Object[] newRow) {
            this.modelName = Iterables.get(Splitter.on('_').split(triggerName), 0);
            mapRecordsFromDB.computeIfAbsent(modelName, m -> new ArrayList<>());
            mapRecordsFromDB.get(modelName).add(LocalDDlogCommand.newLocalDDlogCommand(tableName, newRow));
        }
    }
}
