package org.dcm.viewupdater;

import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import org.dcm.IRColumn;
import org.dcm.IRTable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DerbyUpdater extends ViewUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(DerbyUpdater.class);
    private static DDlogUpdater updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));

    public DerbyUpdater(final DSLContext dbCtx, final List<String> baseTables) {
        super(dbCtx, baseTables);
        createDBTriggers();
    }

    private static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final String update = updater.receiveUpdateFromDDlog(irTables, command);
        if (update != null) {
            UPDATE_QUERIES.add(update);
        }
    }

    public static void sendUpdateToDDlog(final String... args) {
        final DDlogRecord ddlogRecord  = toDDlogRecord(args);
        updater.update(ddlogRecord);
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

    @Override
    public void createDBTriggers() {
        dbCtx.execute("CREATE procedure update_views (vals varchar(100) ...) " +
                "LANGUAGE JAVA " +
                "PARAMETER STYLE DERBY " +
                "NO SQL " +
                "EXTERNAL NAME '" + DerbyUpdater.class.getName() + ".sendUpdateToDDlog'");
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
}