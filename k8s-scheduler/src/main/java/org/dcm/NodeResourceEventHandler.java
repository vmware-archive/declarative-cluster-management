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
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.dcm.k8s.generated.Tables;
import org.dcm.k8s.generated.tables.records.NodeInfoRecord;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Subscribes to Kubernetes node events and reflects them in the database
 */
class NodeResourceEventHandler implements ResourceEventHandler<Node> {
    private static final Logger LOG = LoggerFactory.getLogger(NodeResourceEventHandler.class);
    private final DSLContext conn;

    NodeResourceEventHandler(final DSLContext conn) {
        this.conn = conn;
    }


    @Override
    public void onAdd(final Node node) {
        final long now = System.nanoTime();
        final NodeInfoRecord nodeInfoRecord = conn.newRecord(Tables.NODE_INFO);
        updateNodeRecord(nodeInfoRecord, node);
        addNodeLabels(conn, node);
        addNodeTaints(conn, node);
        addNodeImages(conn, node);
        LOG.info("{} node added in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    @Override
    public void onUpdate(final Node oldNode, final Node newNode) {
        final long now = System.nanoTime();
        final boolean hasChanged = hasChanged(oldNode, newNode);
        if (hasChanged) {
            // TODO: relax this assumption by setting up update cascades for node_info.name FK references
            Preconditions.checkArgument(newNode.getMetadata().getName().equals(oldNode.getMetadata().getName()));
            final NodeInfoRecord nodeInfoRecord = conn.selectFrom(Tables.NODE_INFO)
                    .where(Tables.NODE_INFO.NAME.eq(oldNode.getMetadata().getName()))
                    .fetchOne();
            updateNodeRecord(nodeInfoRecord, newNode);

            if (!Optional.ofNullable(oldNode.getSpec().getTaints())
                    .equals(Optional.ofNullable(newNode.getSpec().getTaints()))) {
                conn.deleteFrom(Tables.NODE_TAINTS)
                        .where(Tables.NODE_TAINTS.NODE_NAME
                                .eq(oldNode.getMetadata().getName()))
                        .execute();
                addNodeTaints(conn, newNode);
            }
            if (!Optional.ofNullable(oldNode.getMetadata().getLabels())
                    .equals(Optional.ofNullable(newNode.getMetadata().getLabels()))) {
                conn.deleteFrom(Tables.NODE_LABELS)
                        .where(Tables.NODE_LABELS.NODE_NAME
                                .eq(oldNode.getMetadata().getName()))
                        .execute();
                addNodeTaints(conn, newNode);
            }
            if (!Optional.ofNullable(oldNode.getStatus().getImages())
                    .equals(Optional.ofNullable(newNode.getStatus().getImages()))) {
                conn.deleteFrom(Tables.NODE_IMAGES)
                        .where(Tables.NODE_IMAGES.NODE_NAME
                                .eq(oldNode.getMetadata().getName()))
                        .execute();
                addNodeTaints(conn, newNode);
            }
        }
        LOG.info("{} => {} node {} in {}ns", oldNode.getMetadata().getName(), newNode.getMetadata().getName(),
                hasChanged ? "updated" : "not updated", (System.nanoTime() - now));
    }

    @Override
    public void onDelete(final Node node, final boolean b) {
        final long now = System.nanoTime();
        deleteNode(node, conn);
        LOG.info("{} node deleted in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    private void updateNodeRecord(final NodeInfoRecord nodeInfoRecord, final Node node) {
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
        nodeInfoRecord.setName(node.getMetadata().getName());
        nodeInfoRecord.setUnschedulable(getUnschedulable);
        nodeInfoRecord.setOutOfDisk(outOfDisk);
        nodeInfoRecord.setMemoryPressure(memoryPressure);
        nodeInfoRecord.setDiskPressure(diskPressure);
        nodeInfoRecord.setPidPressure(pidPressure);
        nodeInfoRecord.setReady(ready);
        nodeInfoRecord.setNetworkUnavailable(networkUnavailable);
        nodeInfoRecord.setCpuCapacity(cpuCapacity);
        nodeInfoRecord.setMemoryCapacity(memoryCapacity);
        nodeInfoRecord.setEphemeralStorageCapacity(ephemeralStorageCapacity);
        nodeInfoRecord.setPodsCapacity(podCapacity);
        nodeInfoRecord.setCpuAllocatable(cpuAllocatable);
        nodeInfoRecord.setMemoryAllocatable(memoryAllocatable);
        nodeInfoRecord.setEphemeralStorageAllocatable(ephemeralStorageAllocatable);
        nodeInfoRecord.setPodsAllocatable(podsAllocatable);

        // These entries are updated by us on every pod arrival/departure
        nodeInfoRecord.setCpuAllocated(zeroIfNull(nodeInfoRecord.getCpuAllocated()));
        nodeInfoRecord.setMemoryAllocated(zeroIfNull(nodeInfoRecord.getMemoryAllocated()));
        nodeInfoRecord.setEphemeralStorageAllocated(zeroIfNull(nodeInfoRecord.getEphemeralStorageAllocated()));
        nodeInfoRecord.setPodsAllocated(zeroIfNull(nodeInfoRecord.getPodsAllocated()));
        nodeInfoRecord.store();
    }

    private long zeroIfNull(@Nullable final Long value) {
        return value == null ? 0 : value;
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

    private void addNodeLabels(final DSLContext conn, final Node node) {
        final Map<String, String> labels = node.getMetadata().getLabels();
        labels.forEach(
                (k, v) -> conn.insertInto(Tables.NODE_LABELS)
                        .values(node.getMetadata().getName(), k, v)
                        .execute()
        );
    }

    private void addNodeTaints(final DSLContext conn, final Node node) {
        if (node.getSpec().getTaints() == null) {
            return;
        }

        for (final Taint taint: node.getSpec().getTaints()) {
            conn.insertInto(Tables.NODE_TAINTS)
                    .values(node.getMetadata().getName(),
                            taint.getKey(),
                            taint.getValue(),
                            taint.getEffect()).execute();
        }
    }

    private void addNodeImages(final DSLContext conn, final Node node) {
        for (final ContainerImage image: node.getStatus().getImages()) {
            for (final String imageName: image.getNames()) {
                final int imageSizeInMb = (int) (((float) image.getSizeBytes()) / 1024 / 1024);
                conn.insertInto(Tables.NODE_IMAGES)
                    .values(node.getMetadata().getName(), imageName, imageSizeInMb).execute();
            }
        }
    }
}
