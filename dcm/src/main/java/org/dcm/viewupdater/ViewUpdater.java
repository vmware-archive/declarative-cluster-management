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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class ViewUpdater {
    String triggerClassName;

    final Connection connection;
    private final List<String> baseTables;
    private final DSLContext dbCtx;
    private final Map<String, Map<String, PreparedStatement>> preparedQueries = new HashMap<>();
    private final DDlogUpdater updater;
    private final Map<String, IRTable> irTables;
    private final Map<String, List<LocalDDlogCommand>> recordsFromDDLog = new HashMap<>();

    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";

    static List<LocalDDlogCommand> recordsFromDB;

    public ViewUpdater(final Connection connection, final DSLContext dbCtx,
                       final List<String> baseTables, final Map<String, IRTable> irTables) {
        recordsFromDB = new ArrayList<>();

        this.connection = connection;
        this.irTables = irTables;
        this.baseTables = baseTables;
        this.dbCtx = dbCtx;
        this.updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r), irTables);
    }

    private String generatePreparedQueryString(final String dataType, final String commandKind) {
        final StringBuilder stringBuilder = new StringBuilder();
        final IRTable irTable = irTables.get(dataType);

        final Table<? extends Record> table = irTable.getTable();
        final Field[] fields = table.fields();
        if (commandKind.equals(String.valueOf(DDlogCommand.Kind.Insert))) {
            stringBuilder.append("insert into ").append(dataType).append(" values ( \n");
            stringBuilder.append(" ?,".repeat(Math.max(0, fields.length - 1)));
            stringBuilder.append(" ?");
        } else if (commandKind.equals(String.valueOf(DDlogCommand.Kind.DeleteVal))) {
            stringBuilder.append("delete from ").append(dataType).append(" where ( \n");
            int counter = 0;
            for (final Field field : fields) {
                stringBuilder.append(field.getName()).append(" = ?");
                if (counter < fields.length - 1) {
                    stringBuilder.append(" and ");
                }
                counter = counter + 1;
            }
        }
        stringBuilder.append("\n)");
        return stringBuilder.toString();
    }

    void createDBTriggers() {
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
                final String triggerName = "TRIGGER_" + tableName;

                final StringBuilder builder = new StringBuilder();
                builder.append("CREATE TRIGGER " + triggerName + " " + "BEFORE INSERT ON " + tableName + " " +
                        "FOR EACH ROW CALL \"" + triggerClassName + "\"");

                final String command = builder.toString();
                System.out.println(command);
                dbCtx.execute(command);
            }
        }
    }

    private void receiveUpdateFromDDlog(final DDlogCommand command) {
        recordsFromDDLog.computeIfAbsent(command.value.getStructName(), k -> new ArrayList<LocalDDlogCommand>());

        final List objects = new ArrayList();
        final DDlogRecord record = command.value;

        final String tableName = command.value.getStructName();
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table = irTable.getTable();

        int counter = 0;
        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
            final DDlogRecord f = record.getStructField(counter);
            switch (cls.getName()) {
                case BOOLEAN_TYPE:
                    objects.add(f.getBoolean());
                    break;
                case INTEGER_TYPE:
                    objects.add(f.getU128());
                    break;
                case LONG_TYPE:
                    objects.add(f.getLong());
                    break;
                case STRING_TYPE:
                    objects.add(f.getString());
                    break;
                default:
                    throw new RuntimeException("Unexpected datatype: " + cls.getName());
            }
            counter = counter + 1;
        }
        recordsFromDDLog.get(tableName).add(new LocalDDlogCommand(command.kind.toString(), tableName, objects));
    }

    public void flushUpdates() {
        updater.sendUpdatesToDDlog(recordsFromDB);
        System.out.println("Size of received requests: " + recordsFromDDLog.keySet());

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
        recordsFromDB.clear();
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
                        query.setLong(index, (Long) item);
                        break;
                    case INTEGER_TYPE:
                        query.setInt(index, (Integer) item);
                        break;
                    case BOOLEAN_TYPE:
                        query.setBoolean(index, (Boolean) item);
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
        final String commandKind = command.command;
        if (!preparedQueries.containsKey(tableName)) {
            preparedQueries.put(tableName, new HashMap<>());
        }
        if (!preparedQueries.get(tableName).containsKey(commandKind)) {
            // make prepared statement here
            @SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
            final String preparedQuery = generatePreparedQueryString(tableName, String.valueOf(command.command));
            try {
                preparedQueries.get(tableName).put(commandKind, connection.prepareStatement(preparedQuery));
            } catch (final SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}