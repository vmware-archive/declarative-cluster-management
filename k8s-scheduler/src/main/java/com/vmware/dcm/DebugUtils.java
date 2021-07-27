/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.k8s.generated.Tables;
import org.apache.commons.io.FileUtils;
import org.jooq.CSVFormat;
import org.jooq.DSLContext;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;


/**
 * Utility methods to dump and reload the state of a database. We can use this to create test cases and reproduce bugs.
 */
class DebugUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DebugUtils.class);
    private static final List<TableImpl<?>> TABLES = List.of(Tables.NODE_INFO,
                                                        Tables.POD_INFO,
                                                        Tables.POD_PORTS_REQUEST,
                                                        Tables.CONTAINER_HOST_PORTS,
                                                        Tables.POD_NODE_SELECTOR_LABELS,
                                                        Tables.POD_AFFINITY_MATCH_EXPRESSIONS,
                                                        Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS,
                                                        Tables.POD_LABELS,
                                                        Tables.NODE_LABELS,
                                                        Tables.VOLUME_LABELS,
                                                        Tables.POD_BY_SERVICE,
                                                        Tables.SERVICE_AFFINITY_LABELS,
                                                        Tables.LABELS_TO_CHECK_FOR_PRESENCE,
                                                        Tables.NODE_TAINTS,
                                                        Tables.POD_TOLERATIONS,
                                                        Tables.NODE_IMAGES,
                                                        Tables.POD_IMAGES);

    // TODO: add folder path as argument
    static void dbDump(final DSLContext conn) {
        for (final TableImpl<?> table: TABLES) {
            try {
                FileUtils.writeStringToFile(new File("/tmp/" + table.getName() + ".csv"),
                        conn.selectFrom(table).fetch().formatCSV(new CSVFormat().nullString("{null}")),
                        StandardCharsets.UTF_8);
            } catch (final IOException e) {
                LOG.error("Could not create db-dump for table {} because of exception:", table.getName(), e);
            }
        }
    }

    // TODO: add folder path as argument
    static void dbLoad(final DSLContext conn) {
        for (final TableImpl<?> table: TABLES) {
            try {
                final String csv = FileUtils.readFileToString(new File("/tmp/" + table.getName() + ".csv"),
                        StandardCharsets.UTF_8);
                conn.loadInto(table).onDuplicateKeyError()
                        .onErrorAbort()
                        .commitAll().loadCSV(csv).fields(table.fields())
                        .nullString("{null}")
                        .separator(',').execute();
            } catch (final IOException e) {
                LOG.error("Could not recover db-dump for table {} because of exception:", table.getName(), e);
            }
        }
    }

}
