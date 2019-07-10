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

import com.vrg.IRTable;

public final class TableRowGenerator extends Qualifier {
    private final IRTable table;

    public TableRowGenerator(final IRTable table) {
        this.table = table;
    }

    public IRTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "TableRowGenerator{" +
                "table=" + table.getName() +
                '}';
    }

    @Override
    void acceptVisitor(final MonoidVisitor visitor) {
        visitor.visitTableRowGenerator(this);
    }
}