/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EmulatedBinder implements IPodToNodeBinder {
    private static final Logger LOG = LoggerFactory.getLogger(EmulatedBinder.class);
    private final DSLContext conn;
    private final ExecutorService executorService = Executors.newScheduledThreadPool(5);

    EmulatedBinder(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public void bindOne(final String namespace, final String podName, final String nodeName) {
        LOG.info("Binding {}/pod:{} to node:{}", namespace, podName, nodeName);

        // Mimic a binding notification
        executorService.execute(() ->
            conn.update(Tables.POD_INFO)
                .set(Tables.POD_INFO.STATUS, "Running")
                .where(Tables.POD_INFO.POD_NAME.eq(podName))
                .execute()
        );
    }
}
