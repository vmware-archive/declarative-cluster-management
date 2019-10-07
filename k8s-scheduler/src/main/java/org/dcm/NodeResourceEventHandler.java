/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.models.V1ContainerImage;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeCondition;
import io.kubernetes.client.models.V1NodeStatus;
import io.kubernetes.client.models.V1Taint;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;

class NodeResourceEventHandler implements ResourceEventHandler<V1Node> {
    private static final Logger LOG = LoggerFactory.getLogger(NodeResourceEventHandler.class);
    private final DSLContext conn;

    NodeResourceEventHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public void onAdd(final V1Node node) {
        LOG.debug("{} node added", node.getMetadata().getName());
        addNodeInfo(node, conn);
    }

    @Override
    public void onUpdate(final V1Node oldNode, final V1Node newNode) {
        LOG.debug("{} => {} node updated", oldNode.getMetadata().getName(), newNode.getMetadata().getName());
    }

    @Override
    public void onDelete(final V1Node node, final boolean deletedFinalStateUnknown) {
        LOG.debug("{} node deleted", node.getMetadata().getName());
        deleteNode(node, conn);
    }

    private void addNodeInfo(final V1Node node, final DSLContext conn) {
        final boolean exists = conn.fetchExists(conn.selectFrom(Tables.NODE_INFO)
                                                    .where(Tables.NODE_INFO.NAME.eq(node.getMetadata().getName())));
        if (exists) {
            LOG.info("Node {} already exists in NODE_INFO table. Ignoring event.", node.getMetadata().getName());
            return;
        }
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
        conn.insertInto(Tables.NODE_INFO)
                .values(
                        node.getMetadata().getName(),
                        node.getMetadata().getLabels().containsKey("node-role.kubernetes.io/master"),
                        getUnschedulable, outOfDisk, memoryPressure, diskPressure,
                        pidPressure, ready, networkUnavailable,
                        capacity.get("cpu").getNumber().multiply(new BigDecimal(1000)), // convert to milli-CPU
                        Utils.convertUnit(capacity.get("memory"), "memory"),
                        Utils.convertUnit(capacity.get("ephemeral-storage"), "ephemeral-storage"),
                        capacity.get("pods").getNumber(),
                        allocatable.get("cpu").getNumber().multiply(new BigDecimal(1000)), // convert to milli-CPU
                        Utils.convertUnit(allocatable.get("memory"), "memory"),
                        Utils.convertUnit(allocatable.get("ephemeral-storage"), "ephmeral-storage"),
                        allocatable.get("pods").getNumber()
                ).execute();
        final Map<String, String> labels = node.getMetadata().getLabels();
        labels.forEach(
                (k, v) -> conn.insertInto(Tables.NODE_LABELS)
                        .values(node.getMetadata().getName(), k, v)
                        .execute()
        );
        updateNodeTaints(node, conn);
        updateNodeImages(node, conn);
    }

    private void deleteNode(final V1Node node, final DSLContext conn) {
        conn.transaction(nested -> {
            final DSLContext txConn = DSL.using(nested);
            txConn.deleteFrom(Tables.NODE_LABELS)
                    .where(Tables.NODE_LABELS.NODE_NAME.eq(node.getMetadata().getName()))
                    .execute();
            txConn.deleteFrom(Tables.NODE_TAINTS)
                    .where(Tables.NODE_TAINTS.NODE_NAME.eq(node.getMetadata().getName()))
                    .execute();
            txConn.deleteFrom(Tables.NODE_IMAGES)
                    .where(Tables.NODE_IMAGES.NODE_NAME.eq(node.getMetadata().getName()))
                    .execute();
            txConn.deleteFrom(Tables.NODE_INFO)
                    .where(Tables.NODE_INFO.NAME.eq(node.getMetadata().getName()))
                    .execute();
        });
        LOG.info("Node {} deleted", node.getMetadata().getName());
    }

    private void updateNodeTaints(final V1Node node, final DSLContext conn) {
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

    private void updateNodeImages(final V1Node node, final DSLContext conn) {
        for (final V1ContainerImage image: node.getStatus().getImages()) {
            for (final String imageName: image.getNames()) {
                final int imageSizeInMb = (int) (((float) image.getSizeBytes()) / 1024 / 1024);
                conn.insertInto(Tables.NODE_IMAGES)
                        .values(node.getMetadata().getName(), imageName, imageSizeInMb).execute();
            }
        }
    }
}
