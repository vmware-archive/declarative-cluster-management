/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;


import com.vmware.dcm.ModelException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


public class IRContext {
    private final Map<String, IRTable> irTables;

    IRContext(final Map<String, IRTable> irTables) {
        this.irTables = new HashMap<>(irTables);
    }

    public Collection<IRTable> getTables() {
        return irTables.values();
    }

    /**
     * Returns the IRTable corresponding to a given table name
     *
     * @param tableName table name to be queried
     * @return the IRTable corresponding to tableName
     */
    public IRTable getTable(final String tableName) {
        final String tableNameCaps = tableName.toUpperCase(Locale.US);
        return Objects.requireNonNull(irTables.get(tableNameCaps), tableNameCaps + " not found");
    }

    /**
     * Returns the IRColumn corresponding to a table name and field name
     *
     * @param tableName table name to be queried
     * @param fieldName field name within tableName
     * @return the IRColumn corresponding to `tableName`.`fieldName`
     */
    public IRColumn getColumn(final String tableName, final String fieldName) {
        final IRTable irTable = getTable(tableName);
        final String fieldNameCaps = fieldName.toUpperCase(Locale.US);
        return Objects.requireNonNull(irTable.getIRColumns().get(fieldNameCaps));
    }

    /**
     * Returns an IRColumn corresponding to a unique field name within a collection of IRTable instances.
     * If the field name is not unique, it throws a ModelException
     *
     * @param fieldName a fieldName to be found within a collection of IRTable instances
     * @param tables a collection of IRTable instances
     * @return Returns a single instance of IRColumn with the given name, within a set of tables.
     * If no instance is found, or if more than one instance of a field with the same name is found within
     * the given set of tables, we throw an exception.
     */
    public IRColumn getColumnIfUnique(final String fieldName, final Collection<IRTable> tables) {
        final List<IRColumn> IRColumns = getColumns(fieldName, tables);

        // we throw an exception if
        // - we find more than one field with the same name in a set of tables
        // - no field was found
        if (IRColumns.size() > 1) {
            throw new ModelException("Ambiguous column (" + fieldName  +
                    ") name in the given set of tables (" + tables + ")");
        }
        if (IRColumns.isEmpty()) {
            throw new ModelException("Column name (" + fieldName  + ") not found in " +
                    "the given set of tables (" + tables + ")");
        }

        // either there are no fields with that name, or we return the only one
        return IRColumns.get(0);
    }

    /**
     * Returns all IRColumn instances that match a field name within a collection of IRTable instances.
     *
     * @param fieldName a fieldName within the IRTable instance
     * @param tables a collection of IRTable instances
     * @return all instances of IRColumn with the given name, within a set of tables.
     */
    private List<IRColumn> getColumns(final String fieldName, final Collection<IRTable> tables) {
        final String fieldNameCaps = fieldName.toUpperCase(Locale.US);
        return tables.stream()
                    .filter(table -> table.getIRColumns().containsKey(fieldNameCaps))
                    .map(table -> table.getIRColumns().get(fieldNameCaps))
                    .collect(Collectors.toList());
    }

    // TODO: this should ideally happen at input time.
    /**
     * Track an IRTable that is either an alias for an existing table or a view table
     * @param table the alias or view table to track
     */
    public void addAliasedOrViewTable(final IRTable table) {
        irTables.putIfAbsent(table.getAliasedName().toUpperCase(Locale.US), table);
    }
}