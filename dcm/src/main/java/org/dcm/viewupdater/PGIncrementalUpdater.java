package org.dcm.viewupdater;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;

import org.dcm.IRTable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.tools.json.JSONObject;
import org.jooq.tools.json.JSONParser;
import org.jooq.tools.json.ParseException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PGIncrementalUpdater extends ViewUpdater {
    private static DDlogUpdater updater = new DDlogUpdater(r -> receiveUpdateFromDDlog(r));

    public PGIncrementalUpdater(final Connection connection, final Map<String, IRTable> irTables,
                                final DSLContext dbCtx, final List<String> baseTables) {
        super(irTables, dbCtx, baseTables);
        final PGConnection conn;
        try {
            conn = connection.unwrap(PGConnection.class);
            conn.addNotificationListener(new PGNotificationListener() {
                @Override
                public void notification(final int processId, final String channelName, final String payload) {
                    if (payload.length() > 0) {
                        final DDlogRecord ddlogRecord = toDDlogRecord(channelName.toUpperCase(Locale.US), payload);
                        updater.update(ddlogRecord);
                    }
                }
            });

            final Statement statement = conn.createStatement();
            for (final String table : ViewUpdater.baseTables) {
                statement.execute("LISTEN " + table.toLowerCase(Locale.US));
                statement.execute("NOTIFY " + table.toLowerCase(Locale.US));
            }
            statement.close();
        } catch (final SQLException e) {
           throw new RuntimeException(e);
        }
    }

    private static void receiveUpdateFromDDlog(final DDlogCommand command) {
        final String update = updater.receiveUpdateFromDDlog(irTables, command);
        if (update != null) {
            UPDATE_QUERIES.add(update);
        }
    }

    @Override
    public void createDBTriggers() {
        dbCtx.execute("CREATE OR REPLACE FUNCTION notify_change() RETURNS TRIGGER AS $$\n" +
                "    BEGIN\n" +
                "        PERFORM pg_notify(TG_TABLE_NAME, row_to_json(NEW)::text);\n" +
                "        RETURN NEW;\n" +
                "    END;\n" +
                " $$ LANGUAGE plpgsql;");

        for (final String entry : baseTables) {
            final String tableName = entry.toUpperCase(Locale.US);
            if (ViewUpdater.irTables.containsKey(tableName)) {
                dbCtx.execute("CREATE TRIGGER TRIGGER_" + tableName + " \n" +
                        "    BEFORE INSERT ON " + tableName + " \n" +
                        "    FOR EACH ROW EXECUTE PROCEDURE notify_change();");
            }
        }
    }

    private static DDlogRecord toDDlogRecord(final String tableName, final String jsonText) {
        final List<DDlogRecord> records = new ArrayList<>();
        final IRTable irTable = irTables.get(tableName);
        final Table<? extends Record> table = irTable.getTable();

        Object obj = null;
        try {
            obj = new JSONParser().parse(jsonText);
        } catch (final ParseException e) {
            throw new RuntimeException(e);
        }
        final JSONObject jsonObject = (JSONObject) obj;

        for (final Field<?> field : table.fields()) {
            final Class<?> cls = field.getType();
            final String fieldName = field.getName().toLowerCase(Locale.US);

            switch (cls.getName()) {
                case BOOLEAN_TYPE:
                    records.add(new DDlogRecord((Boolean) jsonObject.get(fieldName)));
                    break;
                case INTEGER_TYPE:
                case LONG_TYPE:
                    records.add(new DDlogRecord((Long) jsonObject.get(fieldName)));
                    break;
                case STRING_TYPE:
                    records.add(new DDlogRecord((String) jsonObject.get(fieldName)));
                    break;
                default:
                    throw new RuntimeException("Unexpected datatype: " + cls.getName());
            }
        }
        DDlogRecord[] recordsArray = new DDlogRecord[records.size()];
        recordsArray = records.toArray(recordsArray);
        return DDlogRecord.makeStruct(tableName, recordsArray);
    }
}
