/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;


import org.jooq.Field;
import org.jooq.Record;
import org.jooq.UniqueKey;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class IRPrimaryKey {
    private final List<IRColumn> primaryKeyFields;
    private final IRTable irTable;

    public IRPrimaryKey(final IRTable irTable, @Nullable final UniqueKey<? extends Record> pk) {
        this.irTable = irTable;
        this.primaryKeyFields = new ArrayList<>();
        // if no pk just skips - this can happen on views e.g.
        if (pk != null) {
            for (final Field pkField : pk.getFields()) {
                this.primaryKeyFields.add(irTable.getField(pkField));
            }
        }
    }

    /**
     * Returns all the fields that compose this primary key
     * @return all the fields that compose this primary key
     */
    public List<IRColumn> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    /**
     * Returns true if this primaryKey has a controllable field
     * @return true if this primaryKey has a controllable field
     */
    public boolean hasControllableColumn() {
        return primaryKeyFields.stream().anyMatch(IRColumn::isControllable);
    }

    /**
     * Returns the irTable for which this primary key is configured
     * @return the irTable for which this primary key is configured
     */
    public IRTable getIRTable() {
        return irTable;
    }
}
