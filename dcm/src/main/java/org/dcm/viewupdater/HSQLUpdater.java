package org.dcm.viewupdater;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import org.hsqldb.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

public class HSQLUpdater extends ViewUpdater {

    public HSQLUpdater(final Connection connection, final DSLContext dbCtx, final List<String> baseTables,
                       final DDlogAPI api) {
        super(connection, dbCtx, baseTables, api);
        triggerClassName = HSQLUpdater.InnerHSQLUpdater.class.getName();
        createDBTriggers();
    }

    public static class InnerHSQLUpdater implements Trigger {
        private String key;

        public InnerHSQLUpdater() {

        }

        @Override
        public void fire(final int type, final String triggerName,
                         final String tableName, final Object[] oldRow, final Object[] newRow) {
            this.key = Iterables.get(Splitter.on('_').split(triggerName), 0);
            switch (type) {
                case Trigger.INSERT_BEFORE_ROW : {
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.Insert, tableName, newRow));
                    break;
                }
                case Trigger.DELETE_BEFORE_ROW : {
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.DeleteVal, tableName, oldRow));
                    break;
                }
                case Trigger.UPDATE_BEFORE_ROW : {
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.DeleteVal, tableName, oldRow));
                    mapRecordsFromDB.get(key)
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
