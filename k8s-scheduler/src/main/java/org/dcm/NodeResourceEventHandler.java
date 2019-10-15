/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Preconditions;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.models.V1ContainerImage;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeCondition;
import io.kubernetes.client.models.V1NodeStatus;
import io.kubernetes.client.models.V1Taint;
import org.dcm.k8s.generated.Tables;
import org.dcm.k8s.generated.tables.records.NodeInfoRecord;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class NodeResourceEventHandler implements ResourceEventHandler<V1Node> {
    private static final Logger LOG = LoggerFactory.getLogger(NodeResourceEventHandler.class);
    private final DSLContext conn;

    NodeResourceEventHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public void onAdd(final V1Node node) {
        final long now = System.nanoTime();
        final NodeInfoRecord nodeInfoRecord = conn.newRecord(Tables.NODE_INFO);
        updateNodeRecord(nodeInfoRecord, node);
        addNodeLabels(conn, node);
        addNodeTaints(conn, node);
        addNodeImages(conn, node);
        LOG.info("{} node added in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    @Override
    public void onUpdate(final V1Node oldNode, final V1Node newNode) {
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
    public void onDelete(final V1Node node, final boolean deletedFinalStateUnknown) {
        final long now = System.nanoTime();
        deleteNode(node, conn);
        LOG.info("{} node deleted in {}ms", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    private void updateNodeRecord(final NodeInfoRecord nodeInfoRecord, final V1Node node) {
        final V1NodeStatus status = node.getStatus();
        final Map<String, Quantity> capacity = status.getCapacity();
        final Map<String, Quantity> allocatable = status.getAllocatable();
        boolean outOfDisk = false;
        boolean memoryPressure = false;
        boolean diskPressure = false;
        boolean pidPressure = false;
        boolean networkUnavailable = false;
        boolean ready = true;
        final boolean getUnschedulable = node.getSpec().isUnschedulable() != null
                && node.getSpec().isUnschedulable();
        for (final V1NodeCondition condition : status.getConditions()) {
            final boolean value = !condition.getStatus().equals("False");
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
        final long cpuCapacity = capacity.get("cpu").getNumber()
                                         .multiply(new BigDecimal(1000)).longValue(); // convert to milli-cpu
        final long memoryCapacity = (long) Utils.convertUnit(capacity.get("memory"), "memory");
        final long ephemeralStorageCapacity = (long) Utils.convertUnit(capacity.get("ephemeral-storage"),
                                                                         "ephemeral-storage");
        final long podCapacity = capacity.get("pods").getNumber().longValue();
        final long cpuAllocatable = allocatable.get("cpu").getNumber().multiply(new BigDecimal(1000)).longValue();
        final long memoryAllocatable = (long) Utils.convertUnit(allocatable.get("memory"), "memory");
        final long ephemeralStorageAllocatable = (long) Utils.convertUnit(allocatable.get("ephemeral-storage"),
                                                                         "ephemeral-storage");
        final long podsAllocatable = allocatable.get("pods").getNumber().longValue();
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
        nodeInfoRecord.store();
    }

    private boolean hasChanged(final V1Node oldNode, final V1Node newNode) {
        return !oldNode.getSpec().equals(newNode.getSpec())
           || !oldNode.getStatus().getCapacity().equals(newNode.getStatus().getCapacity())
           || !oldNode.getStatus().getAllocatable().equals(newNode.getStatus().getAllocatable())
           || !Optional.ofNullable(oldNode.getSpec().isUnschedulable())
                .equals(Optional.ofNullable(newNode.getSpec().isUnschedulable()))
           || haveConditionsChanged(oldNode, newNode);
    }

    private boolean haveConditionsChanged(final V1Node oldNode, final V1Node newNode) {
        if (oldNode.getStatus().getConditions().size() != newNode.getStatus().getConditions().size()) {
            return true;
        }
        final List<V1NodeCondition> oldConditions = oldNode.getStatus().getConditions();
        final List<V1NodeCondition> newConditions = newNode.getStatus().getConditions();
        for (int i = 0; i < oldNode.getStatus().getConditions().size(); i++) {
            if (!oldConditions.get(i).getStatus().equals(newConditions.get(i).getStatus())) {
                return true;
            }
        }
        return false;
    }


    private void deleteNode(final V1Node node, final DSLContext conn) {
        conn.deleteFrom(Tables.NODE_INFO)
            .where(Tables.NODE_INFO.NAME.eq(node.getMetadata().getName()))
            .execute();
        LOG.info("Node {} deleted", node.getMetadata().getName());
    }

    private void addNodeLabels(final DSLContext conn, final V1Node node) {
        final Map<String, String> labels = node.getMetadata().getLabels();
        labels.forEach(
                (k, v) -> conn.insertInto(Tables.NODE_LABELS)
                        .values(node.getMetadata().getName(), k, v)
                        .execute()
        );
    }

    private void addNodeTaints(final DSLContext conn, final V1Node node) {
        if (node.getSpec().getTaints() == null) {
            return;
        }

        for (final V1Taint taint: node.getSpec().getTaints()) {
            conn.insertInto(Tables.NODE_TAINTS)
                    .values(node.getMetadata().getName(),
                            taint.getKey(),
                            taint.getValue(),
                            taint.getEffect()).execute();
        }
    }

    private void addNodeImages(final DSLContext conn, final V1Node node) {
        for (final V1ContainerImage image: node.getStatus().getImages()) {
            for (final String imageName: image.getNames()) {
                final int imageSizeInMb = (int) (((float) image.getSizeBytes()) / 1024 / 1024);
                conn.insertInto(Tables.NODE_IMAGES)
                    .values(node.getMetadata().getName(), imageName, imageSizeInMb).execute();
            }
        }
    }
}
