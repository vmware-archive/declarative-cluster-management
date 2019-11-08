package org.dcm.viewupdater;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import ddlogapi.DDlogCommand;
import org.dcm.IRTable;
import org.hsqldb.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HSQLUpdater extends ViewUpdater {

    public HSQLUpdater(final Connection connection,
                       final DSLContext dbCtx, final Map<String, IRTable> irTables,
                       final List<String> baseTables) {
        super(connection, dbCtx, baseTables, irTables);
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
            switch (type) {
                case Trigger.INSERT_BEFORE_ROW : {
                    mapRecordsFromDB.get(modelName)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.Insert, tableName, newRow));
                    break;
                }
                case Trigger.DELETE_BEFORE_ROW : {
                    mapRecordsFromDB.get(modelName)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.DeleteVal, tableName, oldRow));
                    break;
                }
                case Trigger.UPDATE_BEFORE_ROW : {
                    mapRecordsFromDB.get(modelName)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.DeleteVal, tableName, oldRow));
                    mapRecordsFromDB.get(modelName)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.Insert, tableName, newRow));
                    break;
                } default: {
                    throw new RuntimeException("Unknown trigger type received from HSQLDB: "
                            + type + " oldRow: " + Arrays.toString(oldRow) + " newRow " + Arrays.toString(newRow));
                }
            }
        }
    }
}
