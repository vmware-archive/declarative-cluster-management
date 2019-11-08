package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DDlogUpdater {
    private final DDlogAPI API;
    private final Map<String, Integer> tableIDMap;
    private final Map<String, IRTable> irTables;
    private final Consumer<DDlogCommand<DDlogRecord>> consumer;

    static final String INTEGER_TYPE = "java.lang.Integer";
    static final String STRING_TYPE = "java.lang.String";
    static final String BOOLEAN_TYPE = "java.lang.Boolean";
    static final String LONG_TYPE = "java.lang.Long";

    public DDlogUpdater(final Consumer<DDlogCommand<DDlogRecord>> consumer, final Map<String, IRTable> irTables) {
        final int ddlogWorkerThreads = 1;
        final boolean storeDataInDDlogBackgroundProgram = false;
        try {
            API = new DDlogAPI(ddlogWorkerThreads, null, storeDataInDDlogBackgroundProgram);
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }

        this.consumer = consumer;
        this.tableIDMap = new HashMap<>();
        this.irTables = irTables;
    }

    private DDlogRecord toDDlogRecord(final String tableName, final Object[] args) {
        final DDlogRecord[] recordsArray = new DDlogRecord[args.length];
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table = irTable.getTable();

        int counter = 0;
        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
            try {
                switch (cls.getName()) {
                    case BOOLEAN_TYPE:
                        recordsArray[counter] = new DDlogRecord((Boolean) args[counter]);
                        break;
                    case INTEGER_TYPE:
                        recordsArray[counter] = new DDlogRecord((Integer) args[counter]);
                        break;
                    case LONG_TYPE:
                        recordsArray[counter] = new DDlogRecord((Long) args[counter]);
                        break;
                    case STRING_TYPE:
                        recordsArray[counter] = new DDlogRecord(args[counter].toString().trim());
                        break;
                    default:
                        throw new RuntimeException("Unexpected datatype: " + cls.getName());
                }
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
            counter = counter + 1;
        }
        DDlogRecord record = null;
        try {
            record = DDlogRecord.makeStruct(tableName, recordsArray);
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
        return record;
    }

    public void sendUpdatesToDDlog(final List<LocalDDlogCommand> commands) {
        final int commandsSize = commands.size();
        final DDlogRecCommand[] ddlogCommands = new DDlogRecCommand[commandsSize];
        int commandIndex = 0;
        for (final LocalDDlogCommand command : commands) {
            final String tableName = command.tableName;
            final List<Object> cmd = command.values;

            int id;
            if (!tableIDMap.containsKey(tableName)) {
                id = API.getTableId(tableName);
                tableIDMap.put(tableName, id);
            }
            id = tableIDMap.get(tableName);

            ddlogCommands[commandIndex] =
                    new DDlogRecCommand(command.command, id, toDDlogRecord(tableName, cmd.toArray()));
            commandIndex++;
        }

        try {
            API.transactionStart();
            API.applyUpdates(ddlogCommands);
            API.transactionCommitDumpChanges(consumer);
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            API.stop();
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
    }
}
