/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelector;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import org.dcm.k8s.generated.Tables;
import org.dcm.k8s.generated.tables.records.PodInfoRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class PodEventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PodEventHandler.class);
    private final DSLContext conn;

    private enum Operators {
        In,
        Exists,
        NotIn,
        DoesNotExists
    }

    PodEventHandler(final DSLContext conn) {
        this.conn = conn;
    }

    PodEvent handlePodEvent(final PodEvent event) {
        switch (event.getAction()) {
            case ADDED:
                addPod(event.getPod());
                break;
            case UPDATED:
                updatePod(event.getPod());
                break;
            case DELETED:
                deletePod(event.getPod());
                break;
            default:
                throw new IllegalArgumentException(event.toString());
        }
        return event;
    }

    private void addPod(final Pod pod) {
        LOG.info("Adding pod {}", pod.getMetadata().getName());
        final PodInfoRecord newPodInfoRecord = conn.newRecord(Tables.POD_INFO);
        updatePodRecord(newPodInfoRecord, pod);
        updateContainerInfoForPod(pod, conn);
        updatePodNodeSelectorLabels(pod, conn);
        updatePodLabels(conn, pod);
        // updateVolumeInfoForPod(pod, pvcToPv, conn);
        updatePodTolerations(pod, conn);
        updatePodAffinity(pod, conn);
    }

    private void deletePod(final Pod pod) {
        LOG.info("Deleting pod {}", pod.getMetadata().getName());
        // The assumption here is that all foreign key references to pod_info.pod_name will be deleted using
        // a delete cascade
        conn.deleteFrom(Tables.POD_INFO)
                .where(Tables.POD_INFO.POD_NAME.eq(pod.getMetadata().getName()))
                .execute();
    }

    private void updatePod(final Pod pod) {
        final PodInfoRecord existingPodInfoRecord = conn.selectFrom(Tables.POD_INFO)
                .where(Tables.POD_INFO.POD_NAME.eq(pod.getMetadata().getName()))
                .fetchOne();
        if (existingPodInfoRecord == null) {
            LOG.trace("Pod {} does not exist. Skipping", pod.getMetadata().getName());
            return;
        }
        LOG.trace("Updating pod {}", pod.getMetadata().getName());
        updatePodRecord(existingPodInfoRecord, pod);
    }

    private void updatePodRecord(final PodInfoRecord podInfoRecord, final Pod pod) {
        final List<ResourceRequirements> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .collect(Collectors.toList());
        final long cpuRequest = (long) Utils.resourceRequirementSum(resourceRequirements, "cpu");
        final long memoryRequest = (long) Utils.resourceRequirementSum(resourceRequirements, "memory");
        final long ephemeralStorageRequest =
                (long) Utils.resourceRequirementSum(resourceRequirements, "ephemeral-storage");
        final long podsRequest = (long) Utils.resourceRequirementSum(resourceRequirements, "pods");
        podInfoRecord.setPodName(pod.getMetadata().getName());
        podInfoRecord.setStatus(pod.getStatus().getPhase());
        podInfoRecord.setNodeName(pod.getSpec().getNodeName());
        podInfoRecord.setNamespace(pod.getMetadata().getNamespace());
        podInfoRecord.setCpuRequest(cpuRequest);
        podInfoRecord.setMemoryRequest(memoryRequest);
        podInfoRecord.setEphemeralStorageRequest(ephemeralStorageRequest);
        podInfoRecord.setPodsRequest(podsRequest);
        // The first owner reference is used to break symmetries.
        final List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
        final String ownerName = (owners == null || owners.size() == 0) ? "" : owners.get(0).getName();
        podInfoRecord.setOwnerName(ownerName);
        podInfoRecord.setCreationTimestamp(pod.getMetadata().getCreationTimestamp());

        final boolean hasNodeSelector = hasNodeSelector(pod);
        podInfoRecord.setHasNodeSelectorLabels(hasNodeSelector);
        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAffinity() != null) {
            podInfoRecord.setHasPodAffinityRequirements(pod.getSpec().getAffinity().getPodAffinity()
                    .getRequiredDuringSchedulingIgnoredDuringExecution().size() > 0);
        } else {
            podInfoRecord.setHasPodAffinityRequirements(false);
        }

        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAntiAffinity() != null) {
            podInfoRecord.setHasPodAntiAffinityRequirements(pod.getSpec().getAffinity().getPodAntiAffinity()
                    .getRequiredDuringSchedulingIgnoredDuringExecution().size() > 0);
        } else {
            podInfoRecord.setHasPodAntiAffinityRequirements(false);
        }

        // We cap the max priority to 100 to prevent overflow issues in the solver
        podInfoRecord.setPriority(Math.min(pod.getSpec().getPriority() == null ? 10 : pod.getSpec().getPriority(),
                100));

        // This field is important because while we injest info about all pods, we only make scheduling decisions
        // for pods that have dcm-scheduler as their name
        podInfoRecord.setSchedulername(pod.getSpec().getSchedulerName());

        podInfoRecord.store(); // upsert
    }

    private boolean hasNodeSelector(final Pod pod) {
        final PodSpec podSpec = pod.getSpec();
        return  (podSpec.getNodeSelector() != null && podSpec.getNodeSelector().size() > 0)
                || (podSpec.getAffinity() != null
                && podSpec.getAffinity().getNodeAffinity() != null
                && podSpec.getAffinity().getNodeAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution() != null
                && podSpec.getAffinity().getNodeAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution()
                .getNodeSelectorTerms().size() > 0);
    }

    private void updateContainerInfoForPod(final Pod pod, final DSLContext conn) {
        for (final Container container: pod.getSpec().getContainers()) {
            if (container.getPorts() == null || container.getPorts().isEmpty()) {
                continue;
            }
            for (final ContainerPort portInfo: container.getPorts()) {
                // This pod has been assigned to a node already. We therefore update the set of host-ports in
                // use at this node
                if (pod.getSpec().getNodeName() != null && portInfo.getHostPort() != null) {
                    conn.insertInto(Tables.CONTAINER_HOST_PORTS)
                            .values(pod.getSpec().getNodeName(),
                                    portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                    portInfo.getHostPort(),
                                    portInfo.getProtocol()).execute();
                }
                // This pod is yet to be assigned to a host, but it has a hostPort requirement. We record
                // this in the pod_ports_request table
                else if (pod.getStatus().getPhase().equals("") && portInfo.getHostPort() != null) {
                    conn.insertInto(Tables.POD_PORTS_REQUEST)
                            .values(pod.getMetadata().getName(),
                                    portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                    portInfo.getHostPort(),
                                    portInfo.getProtocol()).execute();
                }
            }
            conn.insertInto(Tables.POD_IMAGES).values(pod.getMetadata().getName(), container.getImage()).execute();
        }
    }

    private void updatePodNodeSelectorLabels(final Pod pod, final DSLContext conn) {
        // Update pod_node_selector_labels table
        final Map<String, String> nodeSelector = pod.getSpec().getNodeSelector();
        if (nodeSelector != null) {
            // Using a node selector is equivalent to having a single node-selector term and one match expression
            // per selector term
            final int term = 0;
            final int numMatchExpressions = nodeSelector.size();
            int matchExpression = 0;
            for (final Map.Entry<String, String> entry: nodeSelector.entrySet()) {
                matchExpression += 1;
                final String labelKey = entry.getKey();
                final String labelValue = entry.getValue();
                conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                        .values(pod.getMetadata().getName(), term, matchExpression, numMatchExpressions,
                                labelKey, Operators.In.toString(), labelValue)
                        .execute();
            }
        }
    }

    private void updatePodLabels(final DSLContext conn, final Pod pod) {
        // Update pod_labels table. This will be used for managing affinities, I think?
        final Map<String, String> labels = pod.getMetadata().getLabels();
        if (labels != null) {
            final String formatString = "insert into pod_labels values ('%s', '%s', '%s')";
            labels.forEach(
                    (k, v) -> {
                        // TODO: investigate
                        conn.execute(String.format(formatString, pod.getMetadata().getName(), k, v));
                    }
            );
        }
    }

    private void updatePodTolerations(final Pod pod, final DSLContext conn) {
        if (pod.getSpec().getTolerations() == null) {
            return;
        }
        for (final Toleration toleration: pod.getSpec().getTolerations()) {
            conn.insertInto(Tables.POD_TOLERATIONS)
                    .values(pod.getMetadata().getName(),
                            toleration.getKey(),
                            toleration.getValue(),
                            toleration.getEffect() == null ? "Equal" : toleration.getEffect(),
                            toleration.getOperator()).execute();
        }
    }

    private void updatePodAffinity(final Pod pod, final DSLContext conn) {
        final Affinity affinity = pod.getSpec().getAffinity();
        if (affinity == null) {
            return;
        }

        // Node affinity
        if (affinity.getNodeAffinity() != null
                && affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution() != null) {
            final NodeSelector selector =
                    affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            int termNumber = 0;
            for (final NodeSelectorTerm term: selector.getNodeSelectorTerms()) {
                int matchExpressionNumber = 0;
                final int numMatchExpressions = term.getMatchExpressions().size();
                for (final NodeSelectorRequirement expr: term.getMatchExpressions()) {
                    matchExpressionNumber += 1;
                    LOG.info("Pod:{}, Term:{}, MatchExpressionNum:{}, NumMatchExpressions:{}, Key:{}, op:{}, values:{}",
                            pod.getMetadata().getName(), termNumber, matchExpressionNumber, numMatchExpressions,
                            expr.getKey(), expr.getOperator(), expr.getValues());

                    if (expr.getValues() != null) {
                        for (final String value : expr.getValues()) {
                            conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                                    .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber,
                                            numMatchExpressions, expr.getKey(), expr.getOperator(), value).execute();
                        }
                    } else {
                        conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                                .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber,
                                        numMatchExpressions, expr.getKey(), expr.getOperator(), null).execute();
                    }
                }
                termNumber += 1;
            }
        }

        // Pod affinity
        if (affinity.getPodAffinity() != null) {
            insertPodAffinityTerms(Tables.POD_AFFINITY_MATCH_EXPRESSIONS, pod,
                    affinity.getPodAffinity().getRequiredDuringSchedulingIgnoredDuringExecution());
        }

        // Pod Anti affinity
        if (affinity.getPodAntiAffinity() != null) {
            insertPodAffinityTerms(Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS, pod,
                    affinity.getPodAntiAffinity().getRequiredDuringSchedulingIgnoredDuringExecution());
        }
    }

    private <T extends Record> void insertPodAffinityTerms(final Table<T> table, final Pod pod,
                                                           final List<PodAffinityTerm> terms) {
        int termNumber = 0;
        for (final PodAffinityTerm term: terms) {
            int matchExpressionNumber = 0;
            final int numMatchExpressions =  term.getLabelSelector().getMatchExpressions().size();
            for (final LabelSelectorRequirement expr: term.getLabelSelector().getMatchExpressions()) {
                matchExpressionNumber += 1;
                for (final String value: expr.getValues()) {
                    conn.insertInto(table)
                            .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber, numMatchExpressions,
                                    expr.getKey(), expr.getOperator(), value, term.getTopologyKey()).execute();
                }
            }
            termNumber += 1;
        }
    }
}
