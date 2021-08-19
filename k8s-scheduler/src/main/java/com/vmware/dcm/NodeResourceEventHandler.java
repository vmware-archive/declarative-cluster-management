/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.base.Preconditions;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.NodeInfo;
import com.vmware.dcm.k8s.generated.tables.records.NodeImagesRecord;
import com.vmware.dcm.k8s.generated.tables.records.NodeInfoRecord;
import com.vmware.dcm.k8s.generated.tables.records.NodeLabelsRecord;
import com.vmware.dcm.k8s.generated.tables.records.NodeResourcesRecord;
import com.vmware.dcm.k8s.generated.tables.records.NodeTaintsRecord;
import io.fabric8.kubernetes.api.model.ContainerImage;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.vmware.dcm.Utils.convertUnit;


/**
 * Subscribes to Kubernetes node events and reflects them in the database
 */
class NodeResourceEventHandler implements ResourceEventHandler<Node> {
    private static final Logger LOG = LoggerFactory.getLogger(NodeResourceEventHandler.class);
    private final IConnectionPool dbConnectionPool;
    private final ExecutorService service;

    NodeResourceEventHandler(final IConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
        this.service = Executors.newFixedThreadPool(10);
    }

    NodeResourceEventHandler(final IConnectionPool dbConnectionPool, final ExecutorService service) {
        this.dbConnectionPool = dbConnectionPool;
        this.service = service;
    }

    @Override
    public void onAdd(final Node node) {
        service.execute(() -> onAddSync(node));
    }

    @Override
    public void onUpdate(final Node oldNode, final Node newNode) {
        service.execute(() -> onUpdateSync(oldNode, newNode));
    }

    @Override
    public void onDelete(final Node node, final boolean deletedFinalStateUnknown) {
        service.execute(() -> onDeleteSync(node, deletedFinalStateUnknown));
    }

    public void onAddSync(final Node node) {
        final long now = System.nanoTime();
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> queries = new ArrayList<>();
            queries.add(updateNodeRecord(node, conn));
            queries.addAll(addNodeLabels(conn, node));
            queries.addAll(addNodeTaints(conn, node));
            queries.addAll(addNodeImages(conn, node));
            queries.addAll(addNodeCapacities(conn, node));
            conn.batch(queries).execute();
        }
        LOG.info("{} node added in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    public void onUpdateSync(final Node oldNode, final Node newNode) {
        final long now = System.nanoTime();
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> queries = new ArrayList<>();
            // TODO: relax this assumption by setting up update cascades for node_info.name FK references
            Preconditions.checkArgument(newNode.getMetadata().getName().equals(oldNode.getMetadata().getName()));
            if (!Objects.equals(oldNode.getSpec().getTaints(), newNode.getSpec().getTaints())) {
                queries.add(conn.deleteFrom(Tables.NODE_TAINTS).where(Tables.NODE_TAINTS.NODE_NAME
                                .eq(oldNode.getMetadata().getName())));
                queries.addAll(addNodeTaints(conn, newNode));
            }
            if (!Objects.equals(oldNode.getMetadata().getLabels(), newNode.getMetadata().getLabels())) {
                queries.add(conn.deleteFrom(Tables.NODE_LABELS).where(Tables.NODE_LABELS.NODE_NAME
                                .eq(oldNode.getMetadata().getName())));
                queries.addAll(addNodeLabels(conn, newNode));
            }
            if (!Objects.equals(oldNode.getStatus(), newNode.getStatus())) {
                queries.add(conn.deleteFrom(Tables.NODE_RESOURCES).where(Tables.NODE_RESOURCES.UID
                                .eq(oldNode.getMetadata().getUid())));
                queries.add(conn.deleteFrom(Tables.NODE_IMAGES).where(Tables.NODE_IMAGES.NODE_NAME
                                .eq(oldNode.getMetadata().getName())));
                queries.addAll(addNodeCapacities(conn, newNode));
                queries.addAll(addNodeImages(conn, newNode));
            }
            if (queries.size() > 0) {
                queries.add(updateNodeRecord(newNode, conn));
                LOG.info("{} => {} node {} in {}ns", oldNode.getMetadata().getName(), newNode.getMetadata().getName(),
                        "updated", (System.nanoTime() - now));
            } else {
                LOG.info("{} => {} node {} in {}ns", oldNode.getMetadata().getName(), newNode.getMetadata().getName(),
                        "not updated", (System.nanoTime() - now));
            }
            conn.batch(queries).execute();
        }
    }

    public void onDeleteSync(final Node node, final boolean b) {
        final long now = System.nanoTime();
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            deleteNode(node, conn);
        }
        LOG.info("{} node deleted in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    private Insert<NodeInfoRecord> updateNodeRecord(final Node node, final DSLContext conn) {
        final NodeStatus status = node.getStatus();
        boolean outOfDisk = false;
        boolean memoryPressure = false;
        boolean diskPressure = false;
        boolean pidPressure = false;
        boolean networkUnavailable = false;
        boolean ready = true;
        final boolean getUnschedulable = node.getSpec().getUnschedulable() != null
                && node.getSpec().getUnschedulable();
        for (final NodeCondition condition : status.getConditions()) {
            final boolean value = condition.getStatus().equals("True");
            switch (condition.getType()) {
                case "OutOfDisk" -> outOfDisk = value;
                case "MemoryPressure" -> memoryPressure = value;
                case "DiskPressure" -> diskPressure = value;
                case "PIDPressure" -> pidPressure = value;
                case "NetworkUnavailable" -> networkUnavailable = value;
                case "Ready" -> ready = value;
                default -> throw new IllegalStateException("Unknown condition type " + condition.getType());
            }
        }
        final NodeInfo n = Tables.NODE_INFO;
        return conn.insertInto(Tables.NODE_INFO,
                n.UID,
                n.NAME,
                n.UNSCHEDULABLE,
                n.OUT_OF_DISK,
                n.MEMORY_PRESSURE,
                n.DISK_PRESSURE,
                n.PID_PRESSURE,
                n.READY,
                n.NETWORK_UNAVAILABLE)
            .values(node.getMetadata().getUid(),
                    node.getMetadata().getName(),
                    getUnschedulable,
                    outOfDisk,
                    memoryPressure,
                    diskPressure,
                    pidPressure,
                    ready,
                    networkUnavailable
            );
        /*
         * TODO: InsertOnDuplicateSetMoreStep generates a `merge` SQL statement, which isn't currently handled by the
         * SQl->DDlog translator. For now we comment out, but need to address duplicate keys later.
         */
        /*return conn.insertInto(Tables.NODE_INFO,
                n.UID,
                n.NAME,
                n.UNSCHEDULABLE,
                n.OUT_OF_DISK,
                n.MEMORY_PRESSURE,
                n.DISK_PRESSURE,
                n.PID_PRESSURE,
                n.READY,
                n.NETWORK_UNAVAILABLE)
            .values(node.getMetadata().getUid(),
                    node.getMetadata().getName(),
                    getUnschedulable,
                    outOfDisk,
                    memoryPressure,
                    diskPressure,
                    pidPressure,
                    ready,
                    networkUnavailable,
            )
                .onDuplicateKeyUpdate()
            .set(n.UID, node.getMetadata().getUid())
            .set(n.NAME, node.getMetadata().getName())
            .set(n.UNSCHEDULABLE, getUnschedulable)
            .set(n.OUT_OF_DISK, outOfDisk)
            .set(n.MEMORY_PRESSURE, memoryPressure)
            .set(n.DISK_PRESSURE, diskPressure)
            .set(n.PID_PRESSURE, pidPressure)
            .set(n.READY, ready)
            .set(n.NETWORK_UNAVAILABLE, networkUnavailable); */
    }

    private void deleteNode(final Node node, final DSLContext conn) {
        conn.deleteFrom(Tables.NODE_INFO)
            .where(Tables.NODE_INFO.UID.eq(node.getMetadata().getUid()))
            .execute();
        LOG.info("Node {} deleted", node.getMetadata().getName());
    }

    private List<Insert<NodeResourcesRecord>> addNodeCapacities(final DSLContext conn, final Node node) {
        final Map<String, Quantity> allocatable = node.getStatus().getAllocatable();
        return allocatable.entrySet().stream().map(
                (es) -> conn.insertInto(Tables.NODE_RESOURCES)
                            .values(node.getMetadata().getUid(), es.getKey(), convertUnit(es.getValue(), es.getKey()))
                ).collect(Collectors.toList());
    }

    private List<Insert<NodeLabelsRecord>> addNodeLabels(final DSLContext conn, final Node node) {
        final Map<String, String> labels = node.getMetadata().getLabels();
        return labels.entrySet().stream().map(
                (label) -> conn.insertInto(Tables.NODE_LABELS)
                        .values(node.getMetadata().getName(), label.getKey(), label.getValue())
        ).collect(Collectors.toList());
    }

    private List<Insert<NodeTaintsRecord>> addNodeTaints(final DSLContext conn, final Node node) {
        if (node.getSpec().getTaints() == null) {
            return Collections.emptyList();
        }
        return node.getSpec().getTaints().stream().map(taint ->
                    conn.insertInto(Tables.NODE_TAINTS)
                            .values(node.getMetadata().getName(),
                                    taint.getKey(),
                                    taint.getValue() == null ? "" : taint.getValue(),
                                    taint.getEffect())
                ).collect(Collectors.toList());
    }

    private List<Insert<NodeImagesRecord>> addNodeImages(final DSLContext conn, final Node node) {
        final List<Insert<NodeImagesRecord>> inserts = new ArrayList<>();
        for (final ContainerImage image: node.getStatus().getImages()) {
            for (final String imageName: Optional.ofNullable(image.getNames()).orElse(Collections.emptyList())) {
                final int imageSizeInMb = (int) (((float) image.getSizeBytes()) / 1024 / 1024);
                inserts.add(
                    conn.insertInto(Tables.NODE_IMAGES)
                        .values(node.getMetadata().getName(), imageName, imageSizeInMb)
                );
            }
        }
        return inserts;
    }
}
