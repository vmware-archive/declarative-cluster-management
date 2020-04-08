/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ContainerImage;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.dcm.k8s.generated.Tables;
import org.dcm.k8s.generated.tables.NodeInfo;
import org.dcm.k8s.generated.tables.records.NodeImagesRecord;
import org.dcm.k8s.generated.tables.records.NodeInfoRecord;
import org.dcm.k8s.generated.tables.records.NodeLabelsRecord;
import org.dcm.k8s.generated.tables.records.NodeTaintsRecord;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Subscribes to Kubernetes node events and reflects them in the database
 */
class NodeResourceEventHandler implements ResourceEventHandler<Node> {
    private static final Logger LOG = LoggerFactory.getLogger(NodeResourceEventHandler.class);
    private final DBConnectionPool dbConnectionPool;

    NodeResourceEventHandler(final DBConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
    }


    @Override
    public void onAdd(final Node node) {
        final long now = System.nanoTime();
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> queries = new ArrayList<>();
            queries.add(updateNodeRecord(node, conn));
            queries.addAll(addNodeLabels(conn, node));
            queries.addAll(addNodeTaints(conn, node));
            queries.addAll(addNodeImages(conn, node));
            conn.batch(queries).execute();
        }
        LOG.info("{} node added in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    @Override
    public void onUpdate(final Node oldNode, final Node newNode) {
        final long now = System.nanoTime();
        final boolean hasChanged = hasChanged(oldNode, newNode);
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> queries = new ArrayList<>();
            if (hasChanged) {
                // TODO: relax this assumption by setting up update cascades for node_info.name FK references
                Preconditions.checkArgument(newNode.getMetadata().getName().equals(oldNode.getMetadata().getName()));
                queries.add(updateNodeRecord(newNode, conn));

                if (!Optional.ofNullable(oldNode.getSpec().getTaints())
                        .equals(Optional.ofNullable(newNode.getSpec().getTaints()))) {
                    queries.add(
                        conn.deleteFrom(Tables.NODE_TAINTS)
                                .where(Tables.NODE_TAINTS.NODE_NAME
                                        .eq(oldNode.getMetadata().getName()))
                    );
                    queries.addAll(addNodeTaints(conn, newNode));
                }
                if (!Optional.ofNullable(oldNode.getMetadata().getLabels())
                        .equals(Optional.ofNullable(newNode.getMetadata().getLabels()))) {
                    queries.add(conn.deleteFrom(Tables.NODE_LABELS)
                            .where(Tables.NODE_LABELS.NODE_NAME
                                    .eq(oldNode.getMetadata().getName())));
                    queries.addAll(addNodeTaints(conn, newNode));
                }
                if (!Optional.ofNullable(oldNode.getStatus().getImages())
                        .equals(Optional.ofNullable(newNode.getStatus().getImages()))) {
                    queries.add(
                        conn.deleteFrom(Tables.NODE_IMAGES)
                                .where(Tables.NODE_IMAGES.NODE_NAME
                                        .eq(oldNode.getMetadata().getName())));
                    queries.addAll(addNodeTaints(conn, newNode));
                }
            }
            conn.batch(queries).execute();
        }
        LOG.info("{} => {} node {} in {}ns", oldNode.getMetadata().getName(), newNode.getMetadata().getName(),
                hasChanged ? "updated" : "not updated", (System.nanoTime() - now));
    }

    @Override
    public void onDelete(final Node node, final boolean b) {
        final long now = System.nanoTime();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        deleteNode(node, conn);
        LOG.info("{} node deleted in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    private Insert<NodeInfoRecord> updateNodeRecord(final Node node, final DSLContext conn) {
        final NodeStatus status = node.getStatus();
        final Map<String, Quantity> capacity = status.getCapacity();
        final Map<String, Quantity> allocatable = status.getAllocatable();
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
                case "OutOfDisk":
                    outOfDisk = value;
                    break;
                case "MemoryPressure":
                    memoryPressure = value;
                    break;
                case "DiskPressure":
                    diskPressure = value;
                    break;
                case "PIDPressure":
                    pidPressure = value;
                    break;
                case "NetworkUnavailable":
                    networkUnavailable = value;
                    break;
                case "Ready":
                    ready = value;
                    break;
                default:
                    throw new IllegalStateException("Unknown condition type " + condition.getType());
            }
        }

        // TODO: test unit conversions
        final long cpuCapacity = Long.parseLong(capacity.get("cpu").getAmount()) * 1000L;
        final long memoryCapacity = (long) Utils.convertUnit(capacity.get("memory"), "memory");
        final long ephemeralStorageCapacity = (long) Utils.convertUnit(capacity.get("ephemeral-storage"),
                                                                         "ephemeral-storage");
        final long podCapacity = Long.parseLong(capacity.get("pods").getAmount());
        final long cpuAllocatable = Long.parseLong(allocatable.get("cpu").getAmount()) * 1000L;
        final long memoryAllocatable = (long) Utils.convertUnit(allocatable.get("memory"), "memory");
        final long ephemeralStorageAllocatable = (long) Utils.convertUnit(allocatable.get("ephemeral-storage"),
                                                                         "ephemeral-storage");
        final long podsAllocatable = Long.parseLong(allocatable.get("pods").getAmount());

        final NodeInfo n = Tables.NODE_INFO;
        return conn.insertInto(Tables.NODE_INFO,
                n.NAME,
                n.UNSCHEDULABLE,
                n.OUT_OF_DISK,
                n.MEMORY_PRESSURE,
                n.DISK_PRESSURE,
                n.PID_PRESSURE,
                n.READY,
                n.NETWORK_UNAVAILABLE,
                n.CPU_CAPACITY,
                n.MEMORY_CAPACITY,
                n.EPHEMERAL_STORAGE_CAPACITY,
                n.PODS_CAPACITY,
                n.CPU_ALLOCATABLE,
                n.MEMORY_ALLOCATABLE,
                n.EPHEMERAL_STORAGE_ALLOCATABLE,
                n.PODS_ALLOCATABLE,
                n.CPU_ALLOCATED,
                n.MEMORY_ALLOCATED,
                n.EPHEMERAL_STORAGE_ALLOCATED,
                n.PODS_ALLOCATED)
            .values(node.getMetadata().getName(),
                    getUnschedulable,
                    outOfDisk,
                    memoryPressure,
                    diskPressure,
                    pidPressure,
                    ready,
                    networkUnavailable,
                    cpuCapacity,
                    memoryCapacity,
                    ephemeralStorageCapacity,
                    podCapacity,
                    cpuAllocatable,
                    memoryAllocatable,
                    ephemeralStorageAllocatable,
                    podsAllocatable,
                    0L, // cpu allocated default
                    0L, // mem allocated default
                    0L, // ephemeral storage allocated default
                    0L // pods allocated default
            )
            .onDuplicateKeyUpdate()
            .set(n.NAME, node.getMetadata().getName())
            .set(n.UNSCHEDULABLE, getUnschedulable)
            .set(n.OUT_OF_DISK, outOfDisk)
            .set(n.MEMORY_PRESSURE, memoryPressure)
            .set(n.DISK_PRESSURE, diskPressure)
            .set(n.PID_PRESSURE, pidPressure)
            .set(n.READY, ready)
            .set(n.NETWORK_UNAVAILABLE, networkUnavailable)
            .set(n.CPU_CAPACITY, cpuCapacity)
            .set(n.MEMORY_CAPACITY, memoryCapacity)
            .set(n.EPHEMERAL_STORAGE_CAPACITY, ephemeralStorageCapacity)
            .set(n.PODS_CAPACITY, podCapacity)
            .set(n.CPU_ALLOCATABLE, cpuAllocatable)
            .set(n.MEMORY_ALLOCATABLE, memoryAllocatable)
            .set(n.EPHEMERAL_STORAGE_ALLOCATABLE, ephemeralStorageAllocatable)
            .set(n.PODS_ALLOCATABLE, podsAllocatable)

            // These entries are updated by us on every pod arrival/departure
            .set(n.CPU_ALLOCATED, n.CPU_ALLOCATED)
            .set(n.MEMORY_ALLOCATED, n.MEMORY_ALLOCATED)
            .set(n.EPHEMERAL_STORAGE_ALLOCATED, n.EPHEMERAL_STORAGE_ALLOCATED)
            .set(n.PODS_ALLOCATED, n.PODS_ALLOCATED);
    }

    private boolean hasChanged(final Node oldNode, final Node newNode) {
        return !oldNode.getSpec().equals(newNode.getSpec())
           || !oldNode.getStatus().getCapacity().equals(newNode.getStatus().getCapacity())
           || !oldNode.getStatus().getAllocatable().equals(newNode.getStatus().getAllocatable())
           || !Optional.ofNullable(oldNode.getSpec().getUnschedulable())
                .equals(Optional.ofNullable(newNode.getSpec().getUnschedulable()))
           || haveConditionsChanged(oldNode, newNode);
    }

    private boolean haveConditionsChanged(final Node oldNode, final Node newNode) {
        if (oldNode.getStatus().getConditions().size() != newNode.getStatus().getConditions().size()) {
            return true;
        }
        final List<NodeCondition> oldConditions = oldNode.getStatus().getConditions();
        final List<NodeCondition> newConditions = newNode.getStatus().getConditions();
        for (int i = 0; i < oldNode.getStatus().getConditions().size(); i++) {
            if (!oldConditions.get(i).getStatus().equals(newConditions.get(i).getStatus())) {
                return true;
            }
        }
        return false;
    }


    private void deleteNode(final Node node, final DSLContext conn) {
        conn.deleteFrom(Tables.NODE_INFO)
            .where(Tables.NODE_INFO.NAME.eq(node.getMetadata().getName()))
            .execute();
        LOG.info("Node {} deleted", node.getMetadata().getName());
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
                                    taint.getValue(),
                                    taint.getEffect())
                ).collect(Collectors.toList());
    }

    private List<Insert<NodeImagesRecord>> addNodeImages(final DSLContext conn, final Node node) {
        final List<Insert<NodeImagesRecord>> inserts = new ArrayList<>();
        for (final ContainerImage image: node.getStatus().getImages()) {
            for (final String imageName: image.getNames()) {
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
