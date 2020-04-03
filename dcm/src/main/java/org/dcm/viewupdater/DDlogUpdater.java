package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogRecord;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

public class DDlogUpdater {
    private final DDlogAPI api;
    private final Map<String, Integer> tableIDMap;
    private final Map<String, Table<?>> tableMap;
    private final Consumer<DDlogCommand<DDlogRecord>> consumer;

    static final String INTEGER_TYPE = "java.lang.Integer";
    static final String STRING_TYPE = "java.lang.String";
    static final String BOOLEAN_TYPE = "java.lang.Boolean";
    static final String LONG_TYPE = "java.lang.Long";

    public DDlogUpdater(final Consumer<DDlogCommand<DDlogRecord>> consumer, final Map<String, Table<?>> tableMap,
                        final DDlogAPI api) {
        this.api = api;
        this.consumer = consumer;
        this.tableIDMap = new HashMap<>();
        this.tableMap = tableMap;
    }

    private DDlogRecord toDDlogRecord(final String tableName, final Object[] args) {
        final DDlogRecord[] recordsArray = new DDlogRecord[args.length];
        final Table<? extends Record> table = tableMap.get(tableName);

        int fieldIndex = 0;
        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
            try {
                // Handle nullable columns here
                if (args[fieldIndex] == null) {
                    recordsArray[fieldIndex] = DDlogRecord.makeStruct("std.None", new DDlogRecord[0]);
                }
                else {
                    switch (cls.getName()) {
                        case BOOLEAN_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field, new DDlogRecord((Boolean) args[fieldIndex]));
                            break;
                        case INTEGER_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field, new DDlogRecord((Integer) args[fieldIndex]));
                            break;
                        case LONG_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field, new DDlogRecord((Long) args[fieldIndex]));
                            break;
                        case STRING_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field, new DDlogRecord((String) args[fieldIndex]));
                            break;
                        default:
                            throw new RuntimeException(String.format("Unknown datatype %s of field %s in table %s" +
                                    " while sending DB data to DDLog", args[fieldIndex].getClass().getName(),
                                    field.getName(), tableName));
                    }
                }
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
            fieldIndex = fieldIndex + 1;
        }

        try {
            return DDlogRecord.makeStruct("T" + tableName.toLowerCase(Locale.US), recordsArray);
        } catch (final DDlogException | NullPointerException e) {
            throw new RuntimeException(e);
        }
    }

    public DDlogRecord maybeOption(final Field<?> field, final DDlogRecord record) {
        if (field.getDataType().nullable()) {
            try {
                final DDlogRecord[] arr = new DDlogRecord[1];
                arr[0] = record;
                return DDlogRecord.makeStruct("std.Some", arr);
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
        } else {
            return record;
        }
    }

    public void sendUpdatesToDDlog(final List<LocalDDlogCommand> commands) {
        final int commandsSize = commands.size();
        final DDlogRecCommand[] ddlogCommands = new DDlogRecCommand[commandsSize];
        int commandIndex = 0;
        try {
            for (final LocalDDlogCommand command : commands) {
                final String tableName = command.tableName;
                final List<Object> cmd = command.values;

                int id;
                final String ddlogTableName = "R" + tableName.toLowerCase(Locale.US);
                if (!tableIDMap.containsKey(ddlogTableName)) {
                    id = api.getTableId(ddlogTableName);
                    tableIDMap.put(ddlogTableName, id);
                }
                id = tableIDMap.get(ddlogTableName);

                ddlogCommands[commandIndex] =
                        new DDlogRecCommand(command.command, id, toDDlogRecord(tableName, cmd.toArray()));
                commandIndex++;
            }

            api.transactionStart();
            api.applyUpdates(ddlogCommands);
            api.transactionCommitDumpChanges(consumer);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            api.stop();
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
    }
}