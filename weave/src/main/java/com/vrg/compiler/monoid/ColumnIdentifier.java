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

package com.vrg.compiler.monoid;

import com.vrg.IRColumn;

import javax.annotation.Nullable;

public class ColumnIdentifier extends MonoidComprehension {
    private final String tableName;
    private final IRColumn field;
    private final boolean fromGroupByWithDereference;

    public ColumnIdentifier(final String table, final IRColumn field, final boolean fromGroupByWithDereference) {
        this.tableName = table;
        this.field = field;
        this.fromGroupByWithDereference = fromGroupByWithDereference;
    }

    @Override
    public String toString() {
        return String.format("%s[<%s>]", field.getName(), tableName);
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitColumnIdentifier(this, context);
    }

    public IRColumn getField() {
        return field;
    }

    public String getTableName() {
        return field.getIRTable().getAliasedName();
    }

    /**
     * @return true if this column was referenced in a group by using a dereference. False otherwise.
     */
    public boolean fromGroupByWithDereference() {
        return fromGroupByWithDereference;
    }
}