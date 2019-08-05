/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg;


import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
     * @param tableName table name to be queried
     * @return Returns the IRTable corresponding to tableName
     */
    public IRTable getTable(final String tableName) {
        final String tableNameCaps = tableName.toUpperCase(Locale.US);
        if (!irTables.containsKey(tableNameCaps)) {
            throw new Model.WeaveModelException(String.format("Table '%s' not found!", tableName));
        }
        return irTables.get(tableNameCaps);
    }

    /**
     * @param tableName table name to be queried
     * @param fieldName field name within tableName
     * @return Returns the IRColumn corresponding to `tableName`.`fieldName`
     */
    public IRColumn getColumn(final String tableName, final String fieldName) {
        final IRTable irTable = getTable(tableName);
        return getColumn(irTable, fieldName);
    }

    /**
     * @param irTable an IRTable instance
     * @param fieldName a fieldName within the IRTable instance
     * @return Returns the IRColumn based on IRTable.`fieldName`
     */
    private IRColumn getColumn(final IRTable irTable, final String fieldName) {
        final String fieldNameCaps = fieldName.toUpperCase(Locale.US);
        if (!irTable.getIRColumns().containsKey(fieldNameCaps)) {
            throw new Model.WeaveModelException(
                    String.format("Field '%s' not found at table '%s'!", fieldName, irTable.getName()));
        }
        return irTable.getIRColumns().get(fieldNameCaps);
    }

    /**
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
            throw new Model.WeaveModelException("Ambiguous column (" + fieldName  +
                    ") name in the given set of tables (" + tables + ")");
        }
        if (IRColumns.isEmpty()) {
            throw new Model.WeaveModelException("Column name (" + fieldName  + ") not found in " +
                    "the given set of tables (" + tables + ")");
        }

        // either there are no fields with that name, or we return the only one
        return IRColumns.get(0);
    }

    /**
     * @param fieldName a fieldName within the IRTable instance
     * @param tables a collection of IRTable instances
     * @return Returns all instances of IRColumn with the given name, within a set of tables.
     */
    private List<IRColumn> getColumns(final String fieldName, final Collection<IRTable> tables) {
        final String fieldNameCaps = fieldName.toUpperCase(Locale.US);
        return tables.stream()
                    .filter(table -> table.getIRColumns().containsKey(fieldNameCaps))
                    .map(table -> table.getIRColumns().get(fieldNameCaps))
                    .collect(Collectors.toList());
    }

    /*************************
     * Helper methods
     *************************/

    // TODO: this should ideally happen at input time.
    public void addAliasedOrViewTable(final IRTable tableAlias) {
        irTables.put(tableAlias.getAliasedName().toUpperCase(Locale.US), tableAlias);
    }
}
