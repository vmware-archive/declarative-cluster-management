/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.k8s.generated.Tables;
import org.apache.commons.io.FileUtils;
import org.jooq.DSLContext;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;


/**
 * Utility methods to dump and reload the state of a database. We can use this to create test cases and reproduce bugs.
 */
class DebugUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DebugUtils.class);
    private static final List<TableImpl<?>> TABLES = List.of(Tables.NODE_INFO,
                                                        Tables.POD_INFO,
                                                        Tables.POD_PORTS_REQUEST,
                                                        Tables.POD_NODE_SELECTOR_LABELS,
                                                        Tables.POD_AFFINITY_MATCH_EXPRESSIONS,
                                                        Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS,
                                                        Tables.POD_LABELS,
                                                        Tables.NODE_LABELS,
                                                        Tables.NODE_TAINTS,
                                                        Tables.POD_TOLERATIONS,
                                                        Tables.NODE_IMAGES,
                                                        Tables.POD_IMAGES,
                                                        Tables.MATCH_EXPRESSIONS,
                                                        Tables.POD_RESOURCE_DEMANDS,
                                                        Tables.NODE_RESOURCES);

    static void dbDump(final DSLContext conn, final UUID uuid) {
        LOG.error("Creating DB dump with UUID: {}", uuid);
        for (final TableImpl<?> table: TABLES) {
            try {
                FileUtils.writeStringToFile(new File("/tmp/" + uuid, table.getName() + ".json"),
                        conn.selectFrom(table).fetch().formatJSON(),
                        StandardCharsets.UTF_8);
            } catch (final IOException e) {
                LOG.error("Could not create db-dump for table {} because of exception:", table.getName(), e);
            }
        }
    }

    static void dbLoad(final DSLContext conn, final UUID uuid) {
        for (final TableImpl<?> table: TABLES) {
            try {
                final String csv = FileUtils.readFileToString(
                                        new File("/tmp/" + uuid, table.getName() + ".json"),
                                        StandardCharsets.UTF_8);
                conn.loadInto(table).onDuplicateKeyError()
                        .onErrorAbort()
                        .commitAll().loadJSON(csv).fields(table.fields())
                        .execute();
            } catch (final IOException e) {
                LOG.error("Could not recover db-dump for table {} because of exception:", table.getName(), e);
            }
        }
    }
}