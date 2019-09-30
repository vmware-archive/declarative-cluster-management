package org.dcm.viewupdater;

import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.h2.api.Trigger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class H2Updater extends ViewUpdater {
    private static DDlogUpdater updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));

    static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final String update = updater.receiveUpdateFromDDlog(irTables, command);
        if (update != null) {
            UPDATE_QUERIES.add(update);
        }
    }

    public H2Updater(final DSLContext dbCtx, final List<String> baseTables) {
        super(dbCtx, baseTables);
        createDBTriggers();
    }

    public static class InnerH2Updater implements Trigger {
        final Map<String, IRTable> IR_TABLES = ViewUpdater.irTables;
        private String tableName = "";

        @Override
        public void init(final Connection conn, final String schemaName,
                         final String triggerName, final String tableName, final boolean before,
                         final int type) throws SQLException {
            this.tableName = tableName.trim().toUpperCase(Locale.US);
        }

        @Override
        public void fire(final Connection conn, final Object[] old, final Object[] row) {
            final DDlogRecord ddlogRecord  = toDDlogRecord(row);
            updater.update(ddlogRecord);
        }

        DDlogRecord toDDlogRecord(final Object[] args) {
            final List<DDlogRecord> records = new ArrayList<>();
            final IRTable irTable = this.IR_TABLES.get(tableName);
            final Table<? extends Record> table = irTable.getTable();

            int counter = 0;
            for (final Field<?> field : table.fields()) {
                final Class<?> cls = field.getType();
                switch (cls.getName()) {
                    case BOOLEAN_TYPE:
                        records.add(new DDlogRecord((Boolean) args[counter]));
                        break;
                    case INTEGER_TYPE:
                        records.add(new DDlogRecord((Integer) args[counter]));
                        break;
                    case LONG_TYPE:
                        records.add(new DDlogRecord((Long) args[counter]));
                        break;
                    case STRING_TYPE:
                        records.add(new DDlogRecord(args[counter].toString().trim()));
                        break;
                    default:
                        throw new RuntimeException("Unexpected datatype: " + cls.getName());
                }
                counter = counter + 1;
            }
            DDlogRecord[] recordsArray = new DDlogRecord[records.size()];
            recordsArray = records.toArray(recordsArray);
            return DDlogRecord.makeStruct(tableName, recordsArray);
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void remove() {
            // ignore
        }

        DDlogRecord toDDlogRecord(final String tableName, final Object[] args) {
            final List<DDlogRecord> records = new ArrayList<>();
            final IRTable irTable = this.IR_TABLES.get(tableName);
            final Table<? extends Record> table = irTable.getTable();

            int counter = 0;
            for (final Field<?> field : table.fields()) {
                final Class<?> cls = field.getType();
                switch (cls.getName()) {
                    case BOOLEAN_TYPE:
                        records.add(new DDlogRecord((Boolean) args[counter]));
                        break;
                    case INTEGER_TYPE:
                        records.add(new DDlogRecord((Integer) args[counter]));
                        break;
                    case LONG_TYPE:
                        records.add(new DDlogRecord((Long) args[counter]));
                        break;
                    case STRING_TYPE:
                        records.add(new DDlogRecord(args[counter].toString().trim()));
                        break;
                    default:
                        throw new RuntimeException("Unexpected datatype: " + cls.getName());
                }
                counter = counter + 1;
            }
            DDlogRecord[] recordsArray = new DDlogRecord[records.size()];
            recordsArray = records.toArray(recordsArray);
            return DDlogRecord.makeStruct(tableName, recordsArray);
        }
    }

    @Override
    public void createDBTriggers() {
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
                final String triggerName = "TRIGGER_" + tableName;

                final StringBuilder builder = new StringBuilder();
                builder.append("CREATE TRIGGER " + triggerName + " " + "BEFORE INSERT ON " + tableName + " " +
                        "FOR EACH ROW CALL \"" + H2Updater.InnerH2Updater.class.getName() + "\"");

                final String command = builder.toString();
                System.out.println("Command: " + command);
                dbCtx.execute(command);
            }
        }
    }
}
