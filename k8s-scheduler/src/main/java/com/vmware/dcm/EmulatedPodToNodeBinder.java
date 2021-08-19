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
import org.jooq.Delete;
import org.jooq.Record;
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
    private final IConnectionPool dbConnectionPool;
    private final Map<String, SettableFuture<Boolean>> waitForPodBinding = new HashMap<>();

    EmulatedPodToNodeBinder(final IConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
    }

    public Update<PodInfoRecord> bindOne(final String namespace, final String podName, final String podUid,
                                         final String nodeName) {
        LOG.info("Binding {}/pod:{} (uid: {}) to node:{}", namespace, podName, podUid, nodeName);

        // Mimic a binding notification
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            return conn.update(Tables.POD_INFO)
                    .set(Tables.POD_INFO.STATUS, "Running")
                    .where(Tables.POD_INFO.UID.eq(podUid));
        }
    }

    public Delete<PodInfoRecord> unbindOne(final String namespace, final String podName, final String podUid) {
        LOG.info("Delete {}/pod:{} (uid: {})", namespace, podName, podUid);

        // Mimic a binding notification
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            return conn.delete(Tables.POD_INFO)
                       .where(Tables.POD_INFO.UID.eq(podUid));
        }
    }

    public ListenableFuture<Boolean> waitForPodBinding(final String podUid) {
        final SettableFuture<Boolean> settableFuture = SettableFuture.create();
        waitForPodBinding.put(podUid, settableFuture);
        return settableFuture;
    }

    @Override
    public void bindManyAsnc(final List<? extends Record> records) {
        ForkJoinPool.commonPool().execute(
            () -> {
                final List<Update<PodInfoRecord>> updates = records.stream().map(record -> {
                            final String podUid = record.get("UID", String.class);
                            final String podName = record.get("POD_NAME", String.class);
                            final String namespace = record.get("NAMESPACE", String.class);
                            final String nodeName = record.get("CONTROLLABLE__NODE_NAME", String.class);
                            LOG.info("Attempting to bind {}:{} to {} ", namespace, podName, nodeName);
                            return bindOne(namespace, podName, podUid, nodeName);
                        }
                ).collect(Collectors.toList());
                try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                    conn.batch(updates).execute();
                }
                records.forEach(record -> {
                    final String podUid = record.get("UID", String.class);
                    if (waitForPodBinding.containsKey(podUid)) {
                        waitForPodBinding.get(podUid).set(true);
                    }
                });
            }
        );
    }

    @Override
    public void unbindManyAsnc(final List<? extends Record> records) {
        ForkJoinPool.commonPool().execute(
            () -> {
                final List<Delete<PodInfoRecord>> updates = records.stream().map(record -> {
                            final String podUid = record.get("UID", String.class);
                            final String podName = record.get("POD_NAME", String.class);
                            final String namespace = record.get("NAMESPACE", String.class);
                            LOG.info("Attempting to delete {}:{}", namespace, podName);
                            return unbindOne(namespace, podName, podUid);
                        }
                ).collect(Collectors.toList());
                try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                    conn.batch(updates).execute();
                }
            }
        );
    }

    @Override
    public void notifyFail(final Record record) {
        // Unimplemented
    }
}