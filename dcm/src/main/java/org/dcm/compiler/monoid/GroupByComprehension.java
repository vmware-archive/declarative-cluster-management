/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;

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
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitGroupByComprehension(this, context);
    }
}