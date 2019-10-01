package org.dcm.viewupdater;

import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HSQLUpdater extends ViewUpdater {
    private static DDlogUpdater updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));

    public HSQLUpdater(final DSLContext dbCtx, final List<String> baseTables) {
        super(dbCtx, baseTables);
        createDBTriggers();
    }

    static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final String update = updater.receiveUpdateFromDDlog(irTables, command);
        if (update != null) {
            UPDATE_QUERIES.add(update);
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
                        "FOR EACH ROW CALL \"" + InnerHUpdater.class.getName() + "\"");

                final String command = builder.toString();
                dbCtx.execute(command);
            }
        }
    }

    protected static class InnerHUpdater implements org.hsqldb.Trigger {
        final Map<String, IRTable> IR_TABLES = HSQLUpdater.irTables;

        @Override
        public void fire(final int type, final String trigName,
                         final String tabName, final Object[] oldRow, final Object[] newRow) {
            final DDlogRecord ddlogRecord = toDDlogRecord(tabName, newRow);
            updater.update(ddlogRecord);
        }

        DDlogRecord toDDlogRecord(final String tableName, final Object[] args) {
            final List<DDlogRecord> records = new ArrayList<>();
            final IRTable irTable = IR_TABLES.get(tableName);
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
}

/*package org.dcm.viewupdater;

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

public class H2IncrementalUpdater extends ViewUpdater implements Trigger , org.hsqldb.Trigger {
    private static final List<String> UPDATE_QUERIES = new ArrayList<>();
    private String tableName = "";
    private static DDlogUpdater updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));

    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";

    public H2IncrementalUpdater(final Map<String, IRTable> irTables,
                                final DSLContext dbCtx,
                                final List<String> baseTables) {
        super(irTables, dbCtx, baseTables);
        createDBTriggers();
    }

    @Override
    public void init(final Connection conn, final String schemaName,
                     final String triggerName, final String tableName, final boolean before,
                     final int type) throws SQLException {
        this.tableName = tableName.trim().toUpperCase(Locale.US);
    }

    public static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final String update = updater.receiveUpdateFromDDlog(irTables, command);
        if (update != null) {
            UPDATE_QUERIES.add(update);
        }
    }

    @Override
    public void fire(final Connection conn, final Object[] old, final Object[] row) {
        final DDlogRecord ddlogRecord  = toDDlogRecord(row);
        updater.update(ddlogRecord);
    }

    public DDlogRecord toDDlogRecord(final Object[] args) {
        final List<DDlogRecord> records = new ArrayList<>();
        final IRTable irTable = this.irTables.get(tableName);
        final Table<? extends Record> table =  irTable.getTable();

        int counter = 0;
        for (final Field<?> field: table.fields()) {
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

    @Override
    public void createDBTriggers() {
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
                final String triggerName = "TRIGGER_" + tableName;

                final StringBuilder builder = new StringBuilder();
                builder.append("CREATE TRIGGER " + triggerName + " " +
                        "AFTER INSERT ON " + tableName + " " +
                        "FOR EACH ROW " +
                        "CALL \"" + H2IncrementalUpdater.class.getName() + "\"");

                final String command = builder.toString();
                dbCtx.execute(command);
            }
        }
    }

    @Override
    public void flushUpdates() {
        UPDATE_QUERIES.forEach(q -> {
            dbCtx.execute(q);
        });
        UPDATE_QUERIES.clear();
    }

    @Override
    public void fire(final int type, final String trigName,
                     final String tabName, final Object[] oldRow, final Object[] newRow) {
        tableName = tabName;
        final DDlogRecord ddlogRecord  = toDDlogRecord(newRow);
        updater.update(ddlogRecord);
    }
}*/