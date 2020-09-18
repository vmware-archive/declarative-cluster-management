/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.records.PodInfoRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;


/**
 * Applies the results of a pod -> node binding directly against the database
 */
public class EmulatedPodToNodeBinder implements IPodToNodeBinder {
    private static final Logger LOG = LoggerFactory.getLogger(EmulatedPodToNodeBinder.class);
    private final DBConnectionPool dbConnectionPool;
    private final Map<String, SettableFuture<Boolean>> waitForPodBinding = new HashMap<>();

    EmulatedPodToNodeBinder(final DBConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
    }

    public Update<PodInfoRecord> bindOne(final String namespace, final String podName, final String nodeName) {
        LOG.info("Binding {}/pod:{} to node:{}", namespace, podName, nodeName);

        // Mimic a binding notification
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            return conn.update(Tables.POD_INFO)
                    .set(Tables.POD_INFO.STATUS, "Running")
                    .where(Tables.POD_INFO.POD_NAME.eq(podName));
        }
    }

    public ListenableFuture<Boolean> waitForPodBinding(final String podname) {
        final SettableFuture<Boolean> settableFuture = SettableFuture.create();
        waitForPodBinding.put(podname, settableFuture);
        return settableFuture;
    }

    @Override
    public void bindManyAsnc(final Result<? extends Record> records) {
        ForkJoinPool.commonPool().execute(
            () -> {
                final List<Update<PodInfoRecord>> updates = records.stream().map(record -> {
                            final String podName = record.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                            final String namespace = record.get(Tables.PODS_TO_ASSIGN.NAMESPACE);
                            final String nodeName = record.get(Tables.PODS_TO_ASSIGN.CONTROLLABLE__NODE_NAME);
                            LOG.info("Attempting to bind {}:{} to {} ", namespace, podName, nodeName);
                            return bindOne(namespace, podName, nodeName);
                        }
                ).collect(Collectors.toList());
                try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                    conn.batch(updates).execute();
                }
                records.forEach(record -> {
                    final String podName = record.get(Tables.PODS_TO_ASSIGN.POD_NAME);
                    if (waitForPodBinding.containsKey(podName)) {
                        waitForPodBinding.get(podName).set(true);
                    }
                });
            }
        );
    }
}