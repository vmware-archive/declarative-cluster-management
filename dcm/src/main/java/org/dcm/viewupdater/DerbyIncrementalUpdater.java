package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRColumn;
import org.dcm.IRTable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;

public class DerbyIncrementalUpdater extends ViewUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(DerbyIncrementalUpdater.class);
    private static Map<String, Integer> tableIDMap = new HashMap<>();

    private static final DDlogAPI API = new DDlogAPI(1, DerbyIncrementalUpdater::receiveUpdateFromDDlog, false);

    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";

    private static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final DDlogRecord record = command.value;
        final String dataType = record.getStructName();

        if (irTables.containsKey(dataType)) {
            final StringBuilder stringBuilder = new StringBuilder();
            final IRTable irTable = irTables.get(dataType);
            final Table<? extends Record> table = irTable.getTable();
            final Field[] fields = table.fields();
            if (command.kind == DDlogCommand.Kind.Insert) {
                stringBuilder.append("insert into " + dataType + " values ( \n");
                int counter = 0;
                for (final Field field : fields) {
                    final Class fieldClass = field.getType();
                    final DDlogRecord item = record.getStructField(counter);

                    switch (fieldClass.getName()) {
                        case LONG_TYPE:
                            stringBuilder.append(item.getLong());
                            break;
                        case INTEGER_TYPE:
                            stringBuilder.append(item.getU128());
                            break;
                        case BOOLEAN_TYPE:
                            stringBuilder.append(item.getBoolean());
                            break;
                        default:
                            stringBuilder.append("'" + item.getString() + "'");
                    }
                    if (counter < fields.length - 1) {
                        stringBuilder.append(", ");
                    }
                    counter = counter + 1;
                }
            } else if (command.kind == DDlogCommand.Kind.DeleteVal) {
                stringBuilder.append("delete from " + dataType + " where ( \n");
                int counter = 0;
                for (final Field field : fields) {
                    final Class fieldClass = field.getType();
                    final DDlogRecord item = record.getStructField(counter);
                    switch (fieldClass.getName()) {
                        case LONG_TYPE:
                            stringBuilder.append(field.getName() + " = " + item.getLong());
                            break;
                        case INTEGER_TYPE:
                            stringBuilder.append(field.getName() + " = " + item.getU128());
                            break;
                        case BOOLEAN_TYPE:
                            stringBuilder.append(field.getName() + " = " + item.getBoolean());
                            break;
                        default:
                            stringBuilder.append("'" + item.getString() + "'");
                    }
                    if (counter < fields.length - 1) {
                        stringBuilder.append(" and ");
                    }
                    counter = counter + 1;
                }
            }
            stringBuilder.append("\n)");
            final String query = stringBuilder.toString();
            UPDATE_QUERIES.add(query);
        }
    }

    public static void sendUpdateToDDlog(final String... args) {
        final DDlogRecord ddlogRecord  = toDDlogRecord(args);
        final String relation = ddlogRecord.getStructName();

        int id;
        if (tableIDMap.containsKey(relation)) {
            id = tableIDMap.get(relation);
        }
        else  {
            id = API.getTableId(relation);
            tableIDMap.put(relation, id);
        }

        final ArrayList<DDlogCommand> commands = new ArrayList<>();
        commands.add(new DDlogCommand(DDlogCommand.Kind.Insert, id, ddlogRecord));
        final DDlogCommand [] ca = commands.toArray(new DDlogCommand[commands.size()]);

        checkDDlogExitCode(API.start());
        checkDDlogExitCode(API.applyUpdates(ca));
        checkDDlogExitCode(API.commit());
    }

    private static DDlogRecord toDDlogRecord(final String... args) {
        final String tableName = args[0].trim().toUpperCase(Locale.US);
        final List<DDlogRecord> records = new ArrayList<>();
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table =  irTable.getTable();

        int counter = 1;
        for (final Field<?> field: table.fields()) {
            final Class<?> cls = field.getType();
            final String argument = args[counter].trim();
            switch (cls.getName()) {
                case BOOLEAN_TYPE:
                    records.add(new DDlogRecord(Boolean.parseBoolean(argument)));
                    break;
                case INTEGER_TYPE:
                    records.add(new DDlogRecord(Integer.parseInt(argument)));
                    break;
                case LONG_TYPE:
                    records.add(new DDlogRecord(Long.parseLong(argument)));
                    break;
                case STRING_TYPE:
                    records.add(new DDlogRecord(argument));
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

    private static void checkDDlogExitCode(final int exitCode) {
        if (exitCode < 0) {
            throw new RuntimeException("Error executing " + exitCode);
        }
    }

    public void createDBTriggers() {
        dbCtx.execute("CREATE procedure update_views (vals varchar(100) ...) " +
                "LANGUAGE JAVA " +
                "PARAMETER STYLE DERBY " +
                "NO SQL " +
                "EXTERNAL NAME '" + DerbyIncrementalUpdater.class.getName() + ".sendUpdateToDDlog'");
        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (irTables.containsKey(tableName)) {
            final IRTable irTable = irTables.get(tableName);

                final String newTableName = "NEW_" + tableName;
                final String triggerName = "TRIGGER_" + tableName;

                final StringBuilder builder = new StringBuilder();
                builder.append("CREATE TRIGGER " + triggerName + " " +
                        "AFTER INSERT ON " + tableName + " " +
                        "REFERENCING NEW AS " + newTableName + " " +
                        "FOR EACH ROW " +
                        "CALL update_views( " +
                        "cast(cast('" + tableName + "' as char(38)) as varchar(100))");
                for (final Map.Entry<String, IRColumn> column: irTable.getIRColumns().entrySet()) {
                    builder.append(", cast(cast("
                            + newTableName + "." + column.getKey() + " as char(38)) as varchar(100))");
                }
                builder.append(")");
                final String command = builder.toString();
                LOG.info("Trigger: {}", command);
                dbCtx.execute(command);
            }
        }
    }

    public void flushUpdates() {
        UPDATE_QUERIES.forEach(q -> {
            dbCtx.execute(q);
        });
        UPDATE_QUERIES.clear();
    }
}