package org.dcm.viewupdater;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRTable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class DDlogUpdater {

    private DDlogAPI API;
    private static Map<String, Integer> tableIDMap = new HashMap<>();

    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";

    public DDlogUpdater(final Consumer<DDlogCommand> consumer) {
        API = new DDlogAPI(1, consumer, false);
    }

    public String receiveUpdateFromDDlog(final Map<String, IRTable> irTables, final DDlogCommand command) {
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
            return stringBuilder.toString();
        }
        return null;
    }

    public void update(final DDlogRecord ddlogRecord) {
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

    public void checkDDlogExitCode(final int exitCode) {
        if (exitCode < 0) {
            throw new RuntimeException("Error executing " + exitCode);
        }
    }

}
