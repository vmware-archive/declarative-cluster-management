/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg;

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
 * Represents an SQL Table in MiniZinc, and captures some relevant metadata about
 * the table like the primary keys, controllable and input variables. This info
 * is used when creating the minizinc model file.
 *
 * Has to be public so mnz_data.ftl and mnz_model.ftl template files can find the corresponding methods
 */
public class IRTable {
    static final String NUM_ROWS_NAME = "NUM_ROWS";
    private final String name;
    private final String alias;
    @Nullable private final Table<? extends Record> jooqTable;
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
     * @return the original SQL table associated with this IRTable
     */
    public Table<? extends Record> getTable() {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        return jooqTable;
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
     * @return Returns irColumns that have a specific tag
     */
    private List<IRColumn> filterFieldsByTag(final IRColumn.FieldTag filterTag) {
        return irColumns.values().stream()
                .filter(f -> f.getTag() == filterTag)
                .collect(Collectors.toList());
    }

    /**
     * @return Number of rows from the current table
     * We get this by looking at the length of one of the columns
     */
    public int getNumRows() {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        // just returns the length of one of the columns
        return irColumns.values().iterator().next().getFieldValues().size();
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     */
    public String getName() {
        return name;
    }


    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     */
    public String getAliasedName() {
        return alias;
    }

    /**
     * Adds a IRColumn to the table
     */
    public void addField(final IRColumn IRColumn) {
        //add to map with all the irColumns so we update values more easily later
        this.irColumns.put(IRColumn.getName(), IRColumn);
        if (jooqTable != null) {
            this.fieldToIRColumn.put(IRColumn.getJooqField(), IRColumn);
        }
    }


    public Optional<IRPrimaryKey> getPrimaryKey() {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        return primaryKey;
    }

    /**
     * Sets this table primaryKey
     */
    public void setPrimaryKey(final IRPrimaryKey pk) {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        primaryKey = Optional.of(pk);
    }

    /**
     * @return Returns a list of foreign keys, where each one is Map between this table field, and the referenced
     * table foreign key field.
     */
    public List<IRForeignKey> getForeignKeys() {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        return foreignKeys;
    }

    /**
     * Adds a foreign key mapping to this table
     *
     * @param fk Map between this tables irColumns, with the referenced table irColumns
     */
    void addForeignKey(final IRForeignKey fk) {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        this.foreignKeys.add(fk);
    }

    /**
     * @return Returns the IRColumn based on the SQL field
     */
    IRColumn getField(final Field field) {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        return fieldToIRColumn.get(field);
    }

    /**
     * Updates a table field with a list of values
     */
    void updateValues(final Result<? extends Record> recentData) {
        if (jooqTable == null) {
            throw new UnsupportedOperationException("IRTable with null jooq table");
        }
        // stores all the values per field for later use in MiniZinc
        for (final Field<?> field : jooqTable.fields()) {
            fieldToIRColumn.get(field).setValues(recentData.getValues(field));
        }
    }

    @Override
    public String toString() {
        return "IRTable{" +
                "table=" + jooqTable +
                ", name='" + getName() + "'" +
                ", irColumns=" + irColumns +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
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
     * @return Returns all the irColumns of the current table in order
     */
    public Map<String, IRColumn> getIRColumns() {
        return irColumns;
    }
}