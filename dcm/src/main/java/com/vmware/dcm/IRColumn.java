/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.base.Preconditions;
import org.jooq.Field;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;


/**
 * Represents a jooqField within an SQL table used within MiniZinz models
 */
public class IRColumn {
    private static final String FIELD_PREFIX_SEP = "__";

    @Nullable private final Field jooqField;
    @Nullable private final FieldType type;
    @Nullable private final IRTable irTable;
    private final String name;
    private final FieldTag tag;
    @GuardedBy("this") private List<String> values;
    @GuardedBy("this") private List<?> fieldValues;
    private Optional<IRColumn> foreignKeyParent;


    /**
     * In generating models, we distinguish between primary key columns,
     * fixed input columns, and controllable columns. The solver can only vary
     * the value of the latter.
     */
    public enum FieldTag {
        CONTROLLABLE, INPUT
    }

    /**
     * For now, we coerce SQL types to float, int, string, boolean and arrays.
     */
    public enum FieldType {
        FLOAT, INT, STRING, BOOL, ARRAY;

        /**
         * Returns the coerced type of an SQL field
         *
         * @param f SQL table jooqField
         * @return the type of the SQL JooqField
         */
        public static FieldType fromField(final Field<?> f) {
            switch (f.getDataType().getSQLType()) {
                case Types.BIGINT:
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


    /**
     * We add this convenience method so we avoid comparing enums using the string comparison,
     * (e.g. fieldTag == 'CONTROLLABLE') which is the only way we can do it in FreeMaker.
     *
     * @return Returns true if a jooqField is CONTROLLABLE
     */
    public boolean isControllable() {
        return getTag().equals(FieldTag.CONTROLLABLE);
    }

    public boolean isString() {
        return type.equals(FieldType.STRING);
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
        final String fieldName = fieldNameInitial.toUpperCase(Locale.US);

        // by default all fields are input fields
        // but if they have prefixes we find the correct tag after
        FieldTag fieldTag = FieldTag.INPUT;

        if (fieldName.contains(FIELD_PREFIX_SEP)) {
            // if we find a prefix_sep we split jooqField name on that
            final String[] fieldParts = fieldName.split(FIELD_PREFIX_SEP, 2);

            // if the prefix is not one of our own tags, we throw an exception
            try {
                fieldTag = FieldTag.valueOf(fieldParts[0]);
            } catch (final IllegalArgumentException iae) {
                throw new ModelException(
                        String.format("Sequence '%s' is reserved for Weave use to define jooqField prefixes.",
                                FIELD_PREFIX_SEP), iae);
            }

            // Force FIELD_PREFIX_SEP to only exist once: for the tags
            // if even after the prefix we have our FIELD_PREFIX_SEP being used we also throw an exception
            if (fieldParts[1].contains(FIELD_PREFIX_SEP)) {
                throw new ModelException(
                        String.format("Sequence '%s' is reserved for Weave to define jooqField prefixes.",
                                FIELD_PREFIX_SEP));
            }
        }

        // NUM_ROWS jooqField should not be used by the model
        if (fieldName.equals(IRTable.NUM_ROWS_NAME)) {
            throw new ModelException(
                    String.format("Field name '%s' is reserved for Weave.", IRTable.NUM_ROWS_NAME));
        }

        this.name = fieldName;
        this.tag = fieldTag;
        this.type = fieldType;
        this.values = new ArrayList<>();
        this.fieldValues = new ArrayList<>();
        this.foreignKeyParent = Optional.empty();
    }

    /**
     * Returns the IRTable corresponding to this IRColumn
     * @return the IRTable corresponding to this IRColumn
     */
    public IRTable getIRTable() {
        return Preconditions.checkNotNull(irTable);
    }

    /**
     * Returns the Jooq Field that backs this IRColumn
     * @return the Jooq Field that backs this IRColumn
     */
    public Field getJooqField() {
        return Preconditions.checkNotNull(jooqField);
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     * @return the column name
     */
    public String getName() {
        return name;
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     * @return the column's FieldType
     */
    public FieldType getType() {
        return Preconditions.checkNotNull(type);
    }

    /**
     * Sets the foreignKeyParent of this jooqField, if this jooqField is a ForeignKey from another table jooqField
     * @param parent sets the foreign key paraent for this column
     */
    void setForeignKeyParent(final IRColumn parent) {
        Preconditions.checkNotNull(type);
        this.foreignKeyParent = Optional.of(parent);
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     *
     * @return Returns the prefix tag of a jooqField
     */
    FieldTag getTag() {
        return tag;
    }

    /**
     * Returns a list of values (the entire column) corresponding to the IRField
     */
    synchronized List<?> getFieldValues() {
        Preconditions.checkNotNull(type);
        return fieldValues;
    }

    /**
     * This is used to update the IRColumn with the values from its corresponding
     * column in the SQL database.
     *
     * @param values A list of values that will be reflected in the dzn file for
     *               the corresponding jooqField/column.
     */
    synchronized void setValues(final List<?> values) {
        Preconditions.checkNotNull(type);
        this.fieldValues = values;
        this.values = new ArrayList<>();
        for (final Object v : values) {
            // we always convert to string
            String svalue = v == null ? "null" : v.toString();

            /*
             * The generated model differentiates between primary keys, controllable, and
             * fixed columns.
             */
            switch (tag) {
                /*
                 * Both CONTROLLABLE and INPUT have the same rules because current CONTROLLABLE values
                 * are also part of the data model
                 */
                case CONTROLLABLE:
                case INPUT: {
                    switch (type) {
                        // TODO: The following quoting is specific to MiniZinc and need not be done
                        //       this early in the compiler
                        case STRING: {
                            svalue = String.format("\"%s\"", svalue);
                            break;
                        }
                        // bools are lower cased 'true' and 'false'
                        case BOOL: {
                            svalue = svalue.toLowerCase(Locale.US);
                            break;
                        }
                        default: { }
                    }
                    break;
                }
                default: { }
            }
            this.values.add(svalue);
        }
    }

    @Override
    public synchronized String toString() {
        return "IRColumn{" +
                "jooqField=" + this.jooqField +
                ", name='" + this.name + '\'' +
                ", type=" + this.type +
                ", tag=" + this.tag +
                ", values=" + this.values +
                ", fieldValues=" + this.fieldValues +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IRColumn)) {
            return false;
        }
        final IRColumn irColumn = (IRColumn) o;
        return Objects.equals(jooqField, irColumn.jooqField) &&
                type == irColumn.type &&
                Objects.equals(name, irColumn.name) &&
                tag == irColumn.tag &&
                Objects.equals(foreignKeyParent, irColumn.foreignKeyParent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jooqField, type, name, tag, foreignKeyParent);
    }

    public synchronized List<String> getValues() {
        return values;
    }
}
