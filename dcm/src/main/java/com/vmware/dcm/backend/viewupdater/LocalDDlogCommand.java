package com.vmware.dcm.backend.viewupdater;

import ddlogapi.DDlogCommand;

import java.util.Arrays;
import java.util.List;

public class LocalDDlogCommand {
    final DDlogCommand.Kind command;
    final List<Object> values;
    final String tableName;

    LocalDDlogCommand(final DDlogCommand.Kind command, final String tableName, final List<Object> values) {
        this.command = command;
        this.tableName = tableName;
        this.values = values;
    }

    LocalDDlogCommand(final DDlogCommand.Kind command, final String tableName, final Object[] values) {
        this(command, tableName, Arrays.asList(values));
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
