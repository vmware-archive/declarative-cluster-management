package com.vrg;


import org.jooq.Field;
import org.jooq.Record;
import org.jooq.UniqueKey;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class IRPrimaryKey {
    private final List<IRColumn> primaryKeyFields;
    private final IRTable IRTable;

    public IRPrimaryKey(final IRTable IRTable, @Nullable final UniqueKey<? extends Record> pk) {
        this.IRTable = IRTable;
        this.primaryKeyFields = new ArrayList<>();
        // if no pk just skips - this can happen on views e.g.
        if (pk != null) {
            for (final Field pkField : pk.getFields()) {
                this.primaryKeyFields.add(IRTable.getField(pkField));
            }
        }
    }

    /**
     * @return Returns all the fields that compose this primary key
     */
    public List<IRColumn> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    /**
     * @return Returns true if this primaryKey has a controllable field
     */
    public boolean hasControllableColumn() {
        return primaryKeyFields.stream().anyMatch(IRColumn::isControllable);
    }

    public IRTable getIRTable() {
        return IRTable;
    }
}
