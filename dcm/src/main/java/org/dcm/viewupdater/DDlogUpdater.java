package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
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
        API = new DDlogAPI(1, consumer, false);
        API.record_commands("replay.dat", false);
        tableIDMap = new HashMap<>();
        this.irTables = irTables;
    }

    private DDlogRecord toDDlogRecord(final String tableName, final List<Object> args) {
        final List<DDlogRecord> records = new ArrayList<>();
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table = irTable.getTable();

        int counter = 0;
        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
            switch (cls.getName()) {
                case BOOLEAN_TYPE:
                    records.add(new DDlogRecord((Boolean) args.get(counter)));
                    break;
                case INTEGER_TYPE:
                    records.add(new DDlogRecord((Integer) args.get(counter)));
                    break;
                case LONG_TYPE:
                    records.add(new DDlogRecord((Long) args.get(counter)));
                    break;
                case STRING_TYPE:
                    records.add(new DDlogRecord(args.get(counter).toString().trim()));
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

    private DDlogCommand recordToCommand(final LocalDDlogCommand record) {
        tableIDMap.computeIfAbsent(record.tableName, r -> API.getTableId(record.tableName));
        final int id = tableIDMap.get(record.tableName);
        checkDDlogExitCode(id);
        return new DDlogCommand(DDlogCommand.Kind.Insert, id, toDDlogRecord(record.tableName, record.values));
    }

    public void sendUpdatesToDDlog(final List<LocalDDlogCommand> records) {
        final List<DDlogCommand> commands = new ArrayList<>();
        for (final LocalDDlogCommand record: records) {
            commands.add(recordToCommand(record));
        }
        final DDlogCommand [] ca = commands.toArray(new DDlogCommand[commands.size()]);
        checkDDlogExitCode(API.start());
        checkDDlogExitCode(API.applyUpdates(ca));
        checkDDlogExitCode(API.commit());
    }

    private void checkDDlogExitCode(final int exitCode) {
        if (exitCode < 0) {
            throw new RuntimeException("Error executing " + exitCode);
        }
    }
}
