package org.dcm.viewupdater;

import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ViewUpdater can be used to help update views held in the DB incrementally, using DDLog. The class receives triggers
 * based on updates on base tables in the DB. These updates are held in memory until the user calls the function
 * "flushUpdates". Then, these updates are passed to DDlog, that incrementally computes views on them and returns
 * updates. Finally, we push these updates back to the DB.
 */
public abstract class ViewUpdater {
    String triggerClassName;
    final String key;

    final Connection connection;
    private final List<String> baseTables;
    private final DSLContext dbCtx;
    private final Map<String, Map<DDlogCommand.Kind, PreparedStatement>> preparedQueries = new HashMap<>();
    private final DDlogUpdater updater;
    private final Map<String, IRTable> irTables;
    private final Map<String, List<LocalDDlogCommand>> recordsFromDDLog = new HashMap<>();

    private static final String BIGINT_TYPE = "java.math.BigInteger";
    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";

    // connection prefix (per model) -> <List of DDlogRecords per table, per model>
    // correct use requires that a new connection is used for every model
    static Map<String, List<LocalDDlogCommand>> mapRecordsFromDB = new ConcurrentHashMap<>();

    /**
     * @param connection: a connection to the DB used to build prepared statements
     * @param dbCtx: database context, mainly used to create triggers.
     * @param baseTables: the tables we build triggers for
     * @param irTables: the datastructure that gives us schema for the "base" and "view" tables
     */
    public ViewUpdater(final Connection connection, final DSLContext dbCtx,
                       final List<String> baseTables, final Map<String, IRTable> irTables) {
        this.connection = connection;
        // this key is connection-specific and allows us to separate records received from the DB for different models
        this.key = String.format("KEY%d", connection.hashCode());
        mapRecordsFromDB.computeIfAbsent(this.key, m -> new ArrayList<>());

        this.irTables = irTables;
        this.baseTables = baseTables;
        this.dbCtx = dbCtx;
        this.updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r), irTables);
    }

    private String generatePreparedQueryString(final String dataType, final DDlogCommand.Kind commandKind) {
        final StringBuilder stringBuilder = new StringBuilder();
        final IRTable irTable = irTables.get(dataType);
        final Table<? extends Record> table = irTable.getTable();
        final Field[] fields = table.fields();
        if (commandKind == DDlogCommand.Kind.Insert) {
            stringBuilder.append(String.format("insert into %s values ( %n", dataType));
            // for the first fields.length values, use a comma after the ?. No need to put a comma after the last ?
            stringBuilder.append(String.join(" ", "?,".repeat(Math.max(0, fields.length - 1))));
            stringBuilder.append(" ? \n)");
        } else if (commandKind == DDlogCommand.Kind.DeleteVal) {
            stringBuilder.append(String.format("delete from %s where %n", dataType));
            final List<String> fieldNames =
                    Arrays.stream(fields).map(s -> String.format(" %s = ?", s.getName())).collect(Collectors.toList());
            stringBuilder.append(String.join(" and ", fieldNames));
        }
        stringBuilder.append("\n)");
        return stringBuilder.toString();
    }

    void createDBTriggers() {
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
                final String [] operations = {"UPDATE", "INSERT", "DELETE"};
                for (final String op: operations) {
                    final String triggerName = String.format("%s_TRIGGER_%s_%s", key, tableName, op);
                    dbCtx.execute(String.format("CREATE TRIGGER %s BEFORE %s ON %s FOR EACH ROW CALL \"%s\"",
                            triggerName, op, tableName, triggerClassName));
                }
            }
        }
    }

    private void receiveUpdateFromDDlog(final DDlogCommand<DDlogRecord> command) {
        final List objects = new ArrayList();
        final DDlogRecord record = command.value();

        final String tableName = record.getStructName();
        // we only hold records for tables we have in the DB and none others.
        if (irTables.containsKey(tableName)) {
            final IRTable irTable = irTables.get(tableName);
            final Table<? extends Record> table = irTable.getTable();

            int fieldIndex = 0;
            for (final Field<?> field : table.fields()) {
                final Class<?> cls = field.getType();
                final DDlogRecord f = record.getStructField(fieldIndex);
                switch (cls.getName()) {
                    case BOOLEAN_TYPE:
                        objects.add(f.getBoolean());
                        break;
                    case INTEGER_TYPE:
                        objects.add(f.getInt().longValue());
                        break;
                    case LONG_TYPE:
                        objects.add(f.getInt());
                        break;
                    case STRING_TYPE:
                        objects.add(f.getString());
                        break;
                    default:
                        throw new RuntimeException("Unexpected datatype: " + cls.getName());
                }
                fieldIndex = fieldIndex + 1;
            }
            recordsFromDDLog.computeIfAbsent(record.getStructName(), k -> new ArrayList<LocalDDlogCommand>());
            recordsFromDDLog.get(tableName).add(new LocalDDlogCommand(command.kind(), tableName, objects));
        }
    }

    public void flushUpdates() {
        updater.sendUpdatesToDDlog(mapRecordsFromDB.get(key));
         System.out.println("Total Tables: " + recordsFromDDLog.size());
        for (final Map.Entry<String, List<LocalDDlogCommand>> entry: recordsFromDDLog.entrySet()) {
            final String tableName = entry.getKey();
            final List<LocalDDlogCommand> commands = entry.getValue();
                for (final LocalDDlogCommand command : commands) {
                    // check if query is already created and if not, create it
                    if (!preparedQueries.containsKey(tableName) ||
                            (preparedQueries.containsKey(tableName) &&
                                    !preparedQueries.get(tableName).containsKey(command.command))) {
                        updatePreparedQueries(tableName, command);
                    }
                    flush(tableName, command);
                }
        }
        recordsFromDDLog.clear();
        mapRecordsFromDB.get(key).clear();
    }

     private void flush(final String tableName, final LocalDDlogCommand command) {
        try {
            final PreparedStatement query = preparedQueries.get(tableName).get(command.command);
            final IRTable irTable = irTables.get(tableName);
            final Table<? extends Record> table = irTable.getTable();
            final Field[] fields = table.fields();
            for (int i = 0; i < fields.length; i++) {
                final Class fieldClass = fields[i].getType();
                final Object item = command.values.get(i);
                final int index = i + 1;
                switch (fieldClass.getName()) {
                    case LONG_TYPE:
                        if (item.getClass().getName().equals(BIGINT_TYPE)) {
                            query.setInt(index, ((java.math.BigInteger) item).intValue());
                        } else {
                            query.setLong(index, (Long) item);
                        }
                        break;
                    case INTEGER_TYPE:
                        query.setInt(index, (Integer) item);
                        break;
                    case BOOLEAN_TYPE:
                        query.setBoolean(index, (Boolean) item);
                        break;
                    case STRING_TYPE:
                        query.setString(index, (String) item);
                        break;
                    default:
                        query.setString(index, (String) item);
                }
            }
            query.executeUpdate();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

     private void updatePreparedQueries(final String tableName, final LocalDDlogCommand command) {
        final DDlogCommand.Kind commandKind = command.command;
        preparedQueries.computeIfAbsent(tableName, t -> new HashMap<>());
        if (!preparedQueries.get(tableName).containsKey(commandKind)) {
            // make prepared statement here
            final String preparedQuery = generatePreparedQueryString(tableName, command.command);
            try {
                preparedQueries.get(tableName).put(commandKind, connection.prepareStatement(preparedQuery));
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        try {
            updater.close();
            connection.close();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}