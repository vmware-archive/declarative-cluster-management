/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import org.jooq.Field;

import javax.annotation.Nullable;
import java.sql.Types;
import java.util.Locale;
import java.util.Objects;


/**
 * Represents a jooqField within an SQL table
 */
public class IRColumn {
    private static final String VARIABLE_PREFIX = "CONTROLLABLE__";
    @Nullable private final Field<?> jooqField;
    @Nullable private final FieldType type;
    @Nullable private final IRTable irTable;
    private final String name;

    /**
     * For now, we coerce SQL types to float, int, string, boolean and arrays.
     */
    public enum FieldType {
        FLOAT, LONG, INT, STRING, BOOL, ARRAY;

        /**
         * Returns the coerced type of an SQL field
         *
         * @param f SQL table jooqField
         * @return the type of the SQL JooqField
         */
        public static FieldType fromField(final Field<?> f) {
            switch (f.getDataType().getSQLType()) {
                case Types.BIGINT:
                    return FieldType.LONG;
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    return FieldType.INT;
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.NUMERIC:
                case Types.REAL:
                    return FieldType.FLOAT;
                case Types.BIT:
                case Types.BOOLEAN:
                    return FieldType.BOOL;
                case Types.CHAR:
                case Types.VARCHAR:
                    return FieldType.STRING;
                case Types.OTHER:
                    return FieldType.ARRAY;
                default:
                    throw new IllegalArgumentException("Unknown type jooqField: " + f.getDataType().getSQLType());
            }
        }
    }

    public IRColumn(final IRTable irTable, final Field jooqField) {
        this(irTable, jooqField, FieldType.fromField(jooqField), jooqField.getName());
    }

    /**
     * Builds a IRColumn from a SQL jooqField parsing its type and tags
     * @param irTable the IRTable that this column belongs to
     * @param jooqField the Field that this column corresponds to
     * @param fieldType the FieldType of this column
     * @param fieldNameInitial the initial name for this column
     */
    public IRColumn(@Nullable final IRTable irTable, @Nullable final Field jooqField,
                    final FieldType fieldType, final String fieldNameInitial) {
        this.irTable = irTable;
        this.jooqField = jooqField;
        this.name = fieldNameInitial.toUpperCase(Locale.US);
        this.type = fieldType;
    }

    /**
     * @return Returns true if a jooqField is a variable column
     */
    public boolean isControllable() {
        return name.startsWith(VARIABLE_PREFIX);
    }

    public boolean isString() {
        return Objects.requireNonNull(type).equals(FieldType.STRING);
    }

    /**
     * Returns the IRTable corresponding to this IRColumn
     * @return the IRTable corresponding to this IRColumn
     */
    public IRTable getIRTable() {
        return Objects.requireNonNull(irTable);
    }

    /**
     * Returns the Jooq Field that backs this IRColumn
     * @return the Jooq Field that backs this IRColumn
     */
    public Field<?> getJooqField() {
        return Objects.requireNonNull(jooqField);
    }

    /**
     * @return the column name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the column's FieldType
     */
    public FieldType getType() {
        return Objects.requireNonNull(type);
    }

    @Override
    public synchronized String toString() {
        return "IRColumn{" +
                "jooqField=" + this.jooqField +
                ", name='" + this.name + '\'' +
                ", type=" + this.type +
                '}';
    }
}
