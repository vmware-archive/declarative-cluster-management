/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.models.V1Affinity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1LabelSelectorRequirement;
import io.kubernetes.client.models.V1NodeSelector;
import io.kubernetes.client.models.V1NodeSelectorRequirement;
import io.kubernetes.client.models.V1NodeSelectorTerm;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodAffinityTerm;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Toleration;
import io.reactivex.processors.PublishProcessor;
import org.dcm.k8s.generated.Tables;
import org.dcm.k8s.generated.tables.records.PodInfoRecord;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class PodResourceEventHandler implements ResourceEventHandler<V1Pod> {
    private static final Logger LOG = LoggerFactory.getLogger(PodResourceEventHandler.class);

    private enum Operators {
        In,
        Exists,
        NotIn,
        DoesNotExists
    }

    private final DSLContext conn;
    private final PublishProcessor<PodEvent> flowable;

    PodResourceEventHandler(final DSLContext conn, final PublishProcessor<PodEvent> flowable) {
        this.conn = conn;
        this.flowable = flowable;
    }

    @Override
    public void onAdd(final V1Pod pod) {
        if (pod.getSpec().getSchedulerName().equals(Scheduler.SCHEDULER_NAME)) {
            LOG.debug("{} pod added!", pod.getMetadata().getName());
            addPod(conn, pod);
            flowable.onNext(new PodEvent(PodEvent.Action.ADDED, pod)); //
        }
    }

    @Override
    public void onUpdate(final V1Pod oldPod, final V1Pod newPod) {
        final String oldPodScheduler = oldPod.getSpec().getSchedulerName();
        final String newPodScheduler = oldPod.getSpec().getSchedulerName();
        assert oldPodScheduler.equals(newPodScheduler);
        if (newPodScheduler.equals(Scheduler.SCHEDULER_NAME)) {
            LOG.debug("{} => {} pod updated!", oldPod.getMetadata().getName(), newPod.getMetadata().getName());
            updatePod(conn, newPod);
            flowable.onNext(new PodEvent(PodEvent.Action.UPDATED, newPod));
        }
    }

    @Override
    public void onDelete(final V1Pod pod, final boolean deletedFinalStateUnknown) {
        if (pod.getSpec().getSchedulerName().equals(Scheduler.SCHEDULER_NAME)) {
            LOG.debug("{} pod deleted!", pod.getMetadata().getName());
            deletePod(conn, pod);
            flowable.onNext(new PodEvent(PodEvent.Action.DELETED, pod));
        }
    }

    private void addPod(final DSLContext conn, final V1Pod pod) {
        final PodInfoRecord newPodInfoRecord = conn.newRecord(Tables.POD_INFO);
        updatePodRecord(newPodInfoRecord, pod);
        updateContainerInfoForPod(pod, conn);
        updatePodNodeSelectorLabels(pod, conn);
        updatePodLabels(conn, pod);
        // updateVolumeInfoForPod(pod, pvcToPv, conn);
        updatePodTaints(pod, conn);
        updatePodAffinity(pod, conn);
    }

    private void deletePod(final DSLContext conn, final V1Pod pod) {
        // The assumption here is that all foreign key references to pod_info.pod_name will be deleted using
        // a delete cascade
        conn.deleteFrom(Tables.POD_INFO)
                .where(Tables.POD_INFO.POD_NAME.eq(pod.getMetadata().getName()))
                .execute();
        LOG.info("Deleted pod {}", pod.getMetadata().getName());
    }

    private void updatePod(final DSLContext conn, final V1Pod pod) {
        final PodInfoRecord existingPodInfoRecord = conn.selectFrom(Tables.POD_INFO)
                .where(Tables.POD_INFO.POD_NAME.eq(pod.getMetadata().getName()))
                .fetchOne();
        assert existingPodInfoRecord != null;
        LOG.trace("Updating info about pod {}", pod.getMetadata().getName());
        updatePodRecord(existingPodInfoRecord, pod);
    }

    private void updatePodRecord(final PodInfoRecord podInfoRecord, final V1Pod pod) {
        final List<V1ResourceRequirements> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(V1Container::getResources)
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
        final List<V1OwnerReference> owners = pod.getMetadata().getOwnerReferences();
        final String ownerName = (owners == null || owners.size() == 0) ? "" : owners.get(0).getName();
        podInfoRecord.setOwnerName(ownerName);
        podInfoRecord.setCreationTimestamp(pod.getMetadata().getCreationTimestamp().toString());

        final boolean hasNodeSelector = (pod.getSpec().getNodeSelector() != null
                                          && pod.getSpec().getNodeSelector().size() > 0)
                                      || (pod.getSpec().getAffinity().getNodeAffinity()
                                            .getRequiredDuringSchedulingIgnoredDuringExecution() != null
                                        && pod.getSpec().getAffinity().getNodeAffinity()
                                              .getRequiredDuringSchedulingIgnoredDuringExecution()
                                              .getNodeSelectorTerms().size() > 0);
        podInfoRecord.setHasNodeSelectorLabels(hasNodeSelector);
        if (pod.getSpec().getAffinity().getPodAffinity() != null) {
            podInfoRecord.setHasPodAffinityRequirements(pod.getSpec().getAffinity().getPodAffinity()
                                                      .getRequiredDuringSchedulingIgnoredDuringExecution().size() > 0);
        } else {
            podInfoRecord.setHasPodAffinityRequirements(false);
        }

        // We cap the max load to 100 to prevent overflow issues in the solver
        podInfoRecord.setPriority(Math.min(pod.getSpec().getPriority(), 100));
        podInfoRecord.store(); // upsert
    }

    private void updateContainerInfoForPod(final V1Pod pod, final DSLContext conn) {
        for (final V1Container container: pod.getSpec().getContainers()) {
            if (container.getPorts() == null) {
                continue;
            }
            for (final V1ContainerPort portInfo: container.getPorts()) {
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

    private void updatePodNodeSelectorLabels(final V1Pod pod, final DSLContext conn) {
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

    private void updatePodLabels(final DSLContext conn, final V1Pod pod) {
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

    private void updatePodTaints(final V1Pod pod, final DSLContext conn) {
        if (pod.getSpec().getTolerations() == null) {
            return;
        }
        for (final V1Toleration toleration: pod.getSpec().getTolerations()) {
            conn.insertInto(Tables.POD_TOLERATIONS)
                .values(pod.getMetadata().getName(),
                        toleration.getKey(),
                        toleration.getValue(),
                        toleration.getEffect(),
                        toleration.getOperator()).execute();
        }
    }

    private void updatePodAffinity(final V1Pod pod, final DSLContext conn) {
        final V1Affinity affinity = pod.getSpec().getAffinity();
        if (affinity == null) {
            return;
        }

        // Node affinity
        if (affinity.getNodeAffinity() != null
            && affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution() != null) {
            final V1NodeSelector selector =
                    affinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            int termNumber = 0;
            for (final V1NodeSelectorTerm term: selector.getNodeSelectorTerms()) {
                int matchExpressionNumber = 0;
                final int numMatchExpressions = term.getMatchExpressions().size();
                for (final V1NodeSelectorRequirement expr: term.getMatchExpressions()) {
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
            final List<V1PodAffinityTerm> terms =
                    affinity.getPodAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            int termNumber = 0;
            for (final V1PodAffinityTerm term: terms) {
                int matchExpressionNumber = 0;
                final int numMatchExpressions =  term.getLabelSelector().getMatchExpressions().size();
                for (final V1LabelSelectorRequirement expr: term.getLabelSelector().getMatchExpressions()) {
                    matchExpressionNumber += 1;
                    for (final String value: expr.getValues()) {
                        conn.insertInto(Tables.POD_AFFINITY_MATCH_EXPRESSIONS)
                            .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber, numMatchExpressions,
                                    expr.getKey(), expr.getOperator(), value, term.getTopologyKey()).execute();
                    }
                }
                termNumber += 1;
            }
        }

        // Pod Anti affinity
        if (affinity.getPodAntiAffinity() != null) {
            final List<V1PodAffinityTerm> requiredDuringSchedulingIgnoredDuringExecution =
                    affinity.getPodAntiAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            requiredDuringSchedulingIgnoredDuringExecution.forEach(
                term -> term.getLabelSelector().getMatchExpressions().forEach(
                    expr -> expr.getValues().forEach(
                            value -> conn.insertInto(Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS)
                                    .values(pod.getMetadata().getName(),
                                            expr.getKey(), expr.getOperator(), value, term.getTopologyKey()).execute()
                    )
                )
            );
        }
    }
}
