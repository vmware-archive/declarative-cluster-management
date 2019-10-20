package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
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

    static final String INTEGER_TYPE = "java.lang.Integer";
    static final String STRING_TYPE = "java.lang.String";
    static final String BOOLEAN_TYPE = "java.lang.Boolean";
    static final String LONG_TYPE = "java.lang.Long";

    public DDlogUpdater(final Consumer<DDlogCommand> consumer, final Map<String, IRTable> irTables) {
        final int ddlogWorkerThreads = 1;
        final boolean storeDataInDDlogBackgroundProgram = false;
        final boolean appendIfFileExists = false;
        API = new DDlogAPI(ddlogWorkerThreads, consumer, storeDataInDDlogBackgroundProgram);
        API.record_commands("replay.dat", appendIfFileExists);
        tableIDMap = new HashMap<>();
        this.irTables = irTables;
    }

    private DDlogRecord toDDlogRecord(final String tableName, final Object[] args) {
        final DDlogRecord[] recordsArray = new DDlogRecord[args.length];
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table = irTable.getTable();

        int counter = 0;
        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
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
            counter = counter + 1;
        }
        return DDlogRecord.makeStruct(tableName, recordsArray);
    }

    public void sendUpdatesToDDlog(final Map<String, List<Object[]>> commands) {
        final int counter = commands.values().stream().mapToInt(List::size).sum();

        final DDlogCommand[] ddlogCommands = new DDlogCommand[counter];
        int loopCounter = 0;
        for (final Map.Entry<String, List<Object[]>> entry: commands.entrySet()) {
            final String tableName = entry.getKey();
            final List<Object[]> cmds = entry.getValue();

            int id;
            if (!tableIDMap.containsKey(tableName)) {
                id = API.getTableId(tableName);
                checkDDlogExitCode(id);
                tableIDMap.put(tableName, id);
            }
            id = tableIDMap.get(tableName);

            for (int i = 0; i < cmds.size(); i++) {
                ddlogCommands[loopCounter] =
                        new DDlogCommand(DDlogCommand.Kind.Insert, id, toDDlogRecord(tableName, cmds.get(i)));
                loopCounter ++;
            }
        }

        checkDDlogExitCode(API.start());
        checkDDlogExitCode(API.applyUpdates(ddlogCommands));
        checkDDlogExitCode(API.commit());

    }

    private void checkDDlogExitCode(final int exitCode) {
        if (exitCode < 0) {
            throw new RuntimeException("Error executing " + exitCode);
        }
    }

    public void close() {
        API.stop();
    }
}
