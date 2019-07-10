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

public final class GroupByComprehension extends MonoidComprehension {
    private final MonoidComprehension comprehension;
    private final GroupByQualifier groupByQualifier;

    public GroupByComprehension(final MonoidComprehension comprehension, final GroupByQualifier qualifier) {
        this.groupByQualifier = qualifier;
        this.comprehension = comprehension.withQualifier(qualifier);
    }

    @Override
    public String toString() {
        return String.format("[ [i | i in (%s) by group] | group in %s]",
                comprehension, groupByQualifier);
    }

    public MonoidComprehension getComprehension() {
        return comprehension;
    }

    public GroupByQualifier getGroupByQualifier() {
        return groupByQualifier;
    }

    @Override
    void acceptVisitor(final MonoidVisitor visitor) {
        visitor.visitGroupByComprehension(this);
    }
}