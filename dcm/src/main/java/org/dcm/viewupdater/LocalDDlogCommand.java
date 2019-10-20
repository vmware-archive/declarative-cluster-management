package org.dcm.viewupdater;

import java.util.List;

public class LocalDDlogCommand {
    final String command;
    final List<Object> values;
    final String tableName;

    LocalDDlogCommand(final String command, final String tableName, final List<Object> values) {
        this.command = command;
        this.tableName = tableName;
        this.values = values;
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Command: ").append(command).append(" ");
        stringBuilder.append("Tablename: ").append(tableName).append(" ");
        values.forEach(v -> stringBuilder.append(" ").append(v));
        return stringBuilder.toString();
    }
}
