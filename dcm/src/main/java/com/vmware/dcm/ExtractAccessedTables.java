/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Table;

import java.util.Locale;
import java.util.Set;

/**
 * Collects table names for every table referenced within a query.
 */
public class ExtractAccessedTables extends DefaultTraversalVisitor<Void, Void> {
    final Set<String> tableNames;

    ExtractAccessedTables(final Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    protected Void visitTable(final Table node, final Void context) {
        tableNames.add(node.getName().getSuffix().toUpperCase(Locale.getDefault()));
        return super.visitTable(node, context);
    }
}
