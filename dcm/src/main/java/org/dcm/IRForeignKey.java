/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import org.jooq.ForeignKey;
import org.jooq.Record;
import org.jooq.TableField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IRForeignKey {

    private final IRTable childTable;
    private final Map<IRColumn, IRColumn> fields;

    /**
     * Represents an SQL Foreign Key relationship in MiniZinc, and caputres the relationship between the fields from
     * the child table, with the fields from the parent table. The table containing the foreign key is called the
     * child table, and the table containing the candidate key is called the referenced or parent table.
     *
     * Has to be public so mnz_data.ftl and mnz_model.ftl template files can find the corresponding methods
     *
     * @param fk SQL foreign key field
     */
    IRForeignKey(final IRTable childTable, final IRTable parentTable, final ForeignKey<? extends Record, ?> fk) {
        this.childTable = childTable;
        this.fields = new HashMap<>(fk.getFields().size());

        // gets the main table fields and the referenced table fields
        final List<? extends TableField<? extends Record, ?>> childFields = fk.getFields();
        final List<? extends TableField<? extends Record, ?>> parentFields = fk.getKey().getFields();

        // assuming both child and parent fields have the same size
        // SQL shouldn't allow this anyway
        // XXX: verify assumption
        for (int i = 0; i < childFields.size(); i++) {
            final IRColumn childField = childTable.getField(childFields.get(i));
            final IRColumn parentField = parentTable.getField(parentFields.get(i));

            // set the parent and the children fields
            childField.setForeignKeyParent(parentField);

            // map fields from child table to parent table
            this.fields.put(childField, parentField);
        }
    }

    /**
     * @return Returns true if one of the child table fields is a controllable
     */
    private boolean hasControllableField() {
        return fields.keySet().stream().anyMatch(IRColumn::isControllable);
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     */
    public IRTable getChildTable() {
        return childTable;
    }

    /**
     * Used in the mnz_data.ftl and mnz_model.ftl template files
     */
    public Map<IRColumn, IRColumn> getFields() {
        return fields;
    }

    /**
     * @return Returns true if this field has a MiniZinc constraint associated with it
     */
    public boolean hasConstraint() {
        return !this.fields.isEmpty() && this.hasControllableField();
    }
}
