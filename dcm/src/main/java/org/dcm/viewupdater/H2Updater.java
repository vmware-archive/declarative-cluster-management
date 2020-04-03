package org.dcm.viewupdater;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import org.h2.api.Trigger;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class H2Updater extends ViewUpdater {

    public H2Updater(final Connection connection, final DSLContext dbCtx,
                     final List<String> baseTables, final DDlogAPI api) {
        super(connection, dbCtx, baseTables, api);
        triggerClassName = H2Updater.InnerH2Updater.class.getName();
        createDBTriggers();
    }

    public static class InnerH2Updater implements Trigger {
        private String tableName;
        private String key;
        private int type;

        public InnerH2Updater() {

        }

        @Override
        public void init(final Connection connection, final String schemaName, final String triggerName,
                         final String tableName, final boolean before, final int type) throws SQLException {
            this.tableName = tableName;
            this.key = Iterables.get(Splitter.on('_').split(triggerName), 0);
            this.type = type;
        }

        @Override
        public void fire(final Connection connection, final Object[] oldRow,
                         final Object[] newRow) throws SQLException {
            mapRecordsFromDB.computeIfAbsent(key, m -> new ArrayList<>());
            switch (type) {
                case Trigger.INSERT : {
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.Insert, tableName, newRow));
                    break;
                }
                case Trigger.DELETE : {
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.DeleteVal, tableName, oldRow));
                    break;
                }
                case Trigger.UPDATE : {
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.DeleteVal, tableName, oldRow));
                    mapRecordsFromDB.get(key)
                            .add(new LocalDDlogCommand(DDlogCommand.Kind.Insert, tableName, newRow));
                    break;
                } default: {
                    throw new RuntimeException("Unknown trigger type received from H2: "
                            + type + " oldRow: " + Arrays.toString(oldRow) + " newRow " + Arrays.toString(newRow));
                }
            }
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public void remove() throws SQLException {

        }
    }
}
