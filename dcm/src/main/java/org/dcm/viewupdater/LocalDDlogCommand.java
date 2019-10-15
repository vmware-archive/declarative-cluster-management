package org.dcm.viewupdater;

import java.util.ArrayList;
import java.util.List;

public class LocalDDlogCommand {
    final String command;
    final List values;
    final String tableName;

    LocalDDlogCommand(final String command, final String tableName, final List values) {
        this.command = command;
        this.tableName = tableName;
        this.values = values;
    }

    LocalDDlogCommand(final String tableName, final List values) {
        this.tableName = tableName;
        this.values = values;
        this.command = "";
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Command: ").append(command).append(" ");
        stringBuilder.append("Tablename: ").append(tableName).append(" ");
        values.forEach(v -> stringBuilder.append(" ").append(v));
        return stringBuilder.toString();
    }

    public static LocalDDlogCommand newLocalDDlogCommand(final String tableName, final Object[] args) {
        final List<Object> records = new ArrayList<>();
        // first item is the tableName
        for (final Object obj: args) {
            records.add(obj);
        }
        return new LocalDDlogCommand(tableName, records);
    }
}
