/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

public final class GroupByComprehension extends ListComprehension {
    private final ListComprehension comprehension;
    private final GroupByQualifier groupByQualifier;

    public GroupByComprehension(final ListComprehension comprehension, final GroupByQualifier qualifier) {
        this.groupByQualifier = qualifier;
        this.comprehension = comprehension.withQualifier(qualifier);
    }

    @Override
    public String toString() {
        return String.format("[ [i | i in (%s) by group] | group in %s]",
                comprehension, groupByQualifier);
    }

    public ListComprehension getComprehension() {
        return comprehension;
    }

    public GroupByQualifier getGroupByQualifier() {
        return groupByQualifier;
    }

    @Override
    <T, C> T acceptVisitor(final IRVisitor<T, C> visitor, final C context) {
        return visitor.visitGroupByComprehension(this, context);
    }
}