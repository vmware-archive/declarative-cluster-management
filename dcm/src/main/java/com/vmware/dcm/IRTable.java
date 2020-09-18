/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.base.Preconditions;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Represents an SQL Table in our IR, and captures some relevant metadata about
 * the table like the primary keys, controllable and input variables. This info
 * is used by backends when generating models.
 */
public class IRTable {
    static final String NUM_ROWS_NAME = "NUM_ROWS";
    private final String name;
    private final String alias;
    @Nullable private final Table<? extends Record> jooqTable;
    @Nullable private Result<? extends Record> recentData = null;
    private final Map<String, IRColumn> irColumns;
    private final Map<Field, IRColumn> fieldToIRColumn;
    private final List<IRForeignKey> foreignKeys;
    private Optional<IRPrimaryKey> primaryKey = Optional.empty();

    /**
     * Parsing and storing a reference to every field within that jooq table so we can later update the values for
     * those irColumns more easily.
     *
     * @param jooqTable SQL table representation
     */
    public IRTable(final Table<? extends Record> jooqTable) {
        this(jooqTable, jooqTable.getName(), jooqTable.getName());
    }

    public IRTable(@Nullable final Table<? extends Record> jooqTable, final String tableName, final String alias) {
        this.jooqTable = jooqTable;
        this.name = tableName.toUpperCase(Locale.US);
        this.alias = alias.toUpperCase(Locale.US);
        // use LinkedHashMap to preserve insertion order which will be our column order
        this.fieldToIRColumn = new LinkedHashMap<>(jooqTable != null ? jooqTable.fields().length : 0);
        this.irColumns = new LinkedHashMap<>(jooqTable != null ? jooqTable.fields().length : 1);
        this.foreignKeys = new ArrayList<>(jooqTable != null ? jooqTable.getReferences().size() : 0);
    }

    public boolean isAliasedTable() {
        return !alias.equalsIgnoreCase(name);
    }

    public boolean isViewTable() {
        return jooqTable == null;
    }

    public boolean hasVars() {
        for (final Map.Entry<String, IRColumn> entries: irColumns.entrySet()) {
            if (entries.getValue().isControllable()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the Jooq Table associated with this IRTable
     * @return the Jooq Table associated with this IRTable
     */
    public Table<? extends Record> getTable() {
        return Preconditions.checkNotNull(jooqTable);
    }


    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     *
     * @return Get all the irColumns that are variables
     */
    Collection<IRColumn> getVars() {
        return filterFieldsByTag(IRColumn.FieldTag.CONTROLLABLE);
    }

    /**
     * Returns IRColumns that have a specific tag
     */
    private List<IRColumn> filterFieldsByTag(final IRColumn.FieldTag filterTag) {
        return irColumns.values().stream()
                .filter(f -> f.getTag() == filterTag)
                .collect(Collectors.toList());
    }

    /**
     * Returns the number of rows in the table backed by this IRTable
     * @return the number of rows in the table backed by this IRTable
     */
    public int getNumRows() {
        Preconditions.checkNotNull(jooqTable);
        // just returns the length of one of the columns
        return irColumns.values().iterator().next().getFieldValues().size();
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     * @return the table name corresponding to this IRTable
     */
    public String getName() {
        return name;
    }


    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     * @return the alias for this IRTable
     */
    public String getAliasedName() {
        return alias;
    }

    /**
     * Adds a IRColumn to the table
     * @param irColumn the field/column to add to this IRTable
     */
    public void addField(final IRColumn irColumn) {
        //add to map with all the irColumns so we update values more easily later
        this.irColumns.put(irColumn.getName(), irColumn);
        if (jooqTable != null) {
            this.fieldToIRColumn.put(irColumn.getJooqField(), irColumn);
        }
    }

    public Optional<IRPrimaryKey> getPrimaryKey() {
        return Objects.requireNonNull(primaryKey);
    }

    /**
     * Sets this table primaryKey
     * @param pk primary key to set
     */
    public void setPrimaryKey(final IRPrimaryKey pk) {
        Preconditions.checkNotNull(jooqTable);
        primaryKey = Optional.of(pk);
    }

    /**
     * Returns a list of foreign keys, where each one is Map between this table field, and the referenced
     * table foreign key field.
     * @return a list of foreign keys, where each one is Map between this table field, and the referenced
     *        table foreign key field.
     */
    public List<IRForeignKey> getForeignKeys() {
        Preconditions.checkNotNull(jooqTable);
        return foreignKeys;
    }

    /**
     * Adds a foreign key mapping to this table
     *
     * @param fk Map between this tables irColumns, with the referenced table irColumns
     */
    void addForeignKey(final IRForeignKey fk) {
        Preconditions.checkNotNull(jooqTable);
        this.foreignKeys.add(fk);
    }

    /**
     * Returns the IRColumn based on the SQL field
     * @param field field to seaerch for
     * @return the IRColumn corresponding to the `field` parameter
     */
    IRColumn getField(final Field field) {
        Preconditions.checkNotNull(jooqTable);
        return fieldToIRColumn.get(field);
    }

    /**
     * Updates a table field with a list of values
     * @param recentData a result set to update this column to
     */
    void updateValues(final Result<? extends Record> recentData) {
        Preconditions.checkNotNull(jooqTable);
        this.recentData = recentData;
        // stores all the values per field for later use by backends
        for (final Field<?> field : jooqTable.fields()) {
            fieldToIRColumn.get(field).setValues(recentData.getValues(field));
        }
    }

    /**
     * Get the most recently invoked result set for this table.
     * @return the current result set for this column
     */
    public Result<? extends Record> getCurrentData() {
        Preconditions.checkNotNull(recentData);
        return recentData;
    }

    @Override
    public String toString() {
        return "IRTable{" +
                "table=" + jooqTable +
                ", name='" + getName() + "'" +
                ", alias='" + getAliasedName() + "'" +
                ", irColumns=" + irColumns +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IRTable)) {
            return false;
        }
        final IRTable table = (IRTable) o;
        return Objects.equals(name, table.name) &&
                Objects.equals(alias, table.alias) &&
                Objects.equals(jooqTable, table.jooqTable) &&
                Objects.equals(irColumns, table.irColumns) &&
                Objects.equals(foreignKeys, table.foreignKeys) &&
                Objects.equals(primaryKey, table.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, alias, jooqTable, irColumns, foreignKeys, primaryKey);
    }

    /**
     * Returns all the IRColumns of the current table
     * @return all the IRColumns of the current table
     */
    public Map<String, IRColumn> getIRColumns() {
        return irColumns;
    }
}