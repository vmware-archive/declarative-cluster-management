/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelector;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;

import org.dcm.k8s.generated.Tables;
import org.dcm.k8s.generated.tables.PodInfo;
import org.dcm.k8s.generated.tables.records.PodInfoRecord;
import org.dcm.k8s.generated.tables.records.PodLabelsRecord;
import org.dcm.k8s.generated.tables.records.PodNodeSelectorLabelsRecord;
import org.dcm.k8s.generated.tables.records.PodTolerationsRecord;
import org.h2.api.Trigger;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Reflects pod events from the Kubernetes API into the database.
 */
class PodEventsToDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(PodEventsToDatabase.class);
    private final DBConnectionPool dbConnectionPool;
    boolean hasAffinityRequirements = false;
    boolean hasAntiAffinityRequirements = false;

    private enum Operators {
        In,
        Exists,
        NotIn,
        DoesNotExist
    }

    PodEventsToDatabase(final DBConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            conn.execute("create or replace trigger nodeInfoResourceUpdateOnInsert " +
                         "after insert on pod_info for each row " +
                         "call \"" + NodeInfoIncrementalUpdate.class.getName() + "\"");
            conn.execute("create or replace trigger nodeInfoResourceUpdateOnUpdate " +
                         "after update on pod_info for each row " +
                         "call \"" + NodeInfoIncrementalUpdate.class.getName() + "\"");
            conn.execute("create or replace trigger nodeInfoResourceUpdateOnDelete " +
                          "after delete on pod_info for each row " +
                          "call \"" + NodeInfoIncrementalUpdate.class.getName() + "\"");
        } catch (final DataAccessException e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    /**
     * This trigger is used to incrementally reflect pod resource requests in the corresponding
     * node tables.
     */
    public static class NodeInfoIncrementalUpdate implements Trigger {

        @Override
        public void init(final Connection connection, final String s,
                         final String s1, final String s2, final boolean b, final int i) {
        }

        @Override
        public void fire(final Connection connection, final Object[] oldRow, final Object[] newRow)
                         throws SQLException {
            try (final PreparedStatement stmt = connection.prepareStatement(
                    "update node_info set node_info.cpu_allocated = node_info.cpu_allocated + ?," +
                     "node_info.memory_allocated = node_info.memory_allocated + ?," +
                     "node_info.ephemeral_storage_allocated = node_info.ephemeral_storage_allocated + ?," +
                     "node_info.pods_allocated = node_info.pods_allocated + ? " +
                     "where node_info.name = ?")) {
                final boolean isInsert = (oldRow == null && newRow != null && newRow[2] != null);
                final boolean isDeletion = (oldRow != null && newRow == null && oldRow[2] != null);
                final boolean isNodeNameUpdate =
                        oldRow != null && newRow != null && oldRow[2] == null && newRow[2] != null;
                if (isDeletion) {
                    applyPodInfoUpdateAgainstNode(stmt, oldRow, (String) oldRow[2], true);
                } else if (isInsert || isNodeNameUpdate) {
                    applyPodInfoUpdateAgainstNode(stmt, newRow, (String) newRow[2], false);
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void remove() {
        }

        private void applyPodInfoUpdateAgainstNode(final PreparedStatement statement, final Object[] row,
                                                   final String nodeName, final boolean isDeletion)
                                                   throws SQLException {
            final int sign = isDeletion ? -1 : 1;
            statement.setLong(1, sign * ((long) row[4])); // CPU_REQUEST
            statement.setLong(2, sign * ((long) row[5])); // MEMORY_REQUEST
            statement.setLong(3, sign * ((long) row[6])); // EPHEMERAL_STORAGE_REQUEST
            statement.setLong(4, sign * ((long) row[7])); // PODS_REQUEST
            statement.setString(5, nodeName);
            statement.execute();
        }
    }

    PodEvent handle(final PodEvent event) {
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
        LOG.trace("Adding pod {}", pod.getMetadata().getName());
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> inserts = new ArrayList<>();
            final List<Insert<?>> podAffinityQueries = updatePodAffinity(pod, conn);
            inserts.addAll(updatePodRecord(pod, conn));
            inserts.addAll(updateContainerInfoForPod(pod, conn));
            inserts.addAll(updatePodNodeSelectorLabels(pod, conn));
            inserts.addAll(updatePodLabels(conn, pod));
            // updateVolumeInfoForPod(pod, pvcToPv, conn);
            inserts.addAll(updatePodTolerations(pod, conn));
            inserts.addAll(podAffinityQueries);
            conn.batch(inserts).execute();
        }
        hasAffinityRequirements = false;
        hasAntiAffinityRequirements = false;
    }

    private void deletePod(final Pod pod) {
        LOG.trace("Deleting pod {}", pod.getMetadata().getName());
        // The assumption here is that all foreign key references to pod_info.pod_name will be deleted using
        // a delete cascade
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            conn.deleteFrom(Tables.POD_INFO)
                .where(Tables.POD_INFO.POD_NAME.eq(pod.getMetadata().getName())).execute();
        }
    }

    private void updatePod(final Pod pod) {
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final PodInfoRecord existingPodInfoRecord = conn.selectFrom(Tables.POD_INFO)
                    .where(Tables.POD_INFO.POD_NAME.eq(pod.getMetadata().getName()))
                    .fetchOne();
            if (existingPodInfoRecord == null) {
                LOG.trace("Pod {} does not exist. Skipping", pod.getMetadata().getName());
                return;
            }
            LOG.trace("Updating pod {}", pod.getMetadata().getName());
            final List<Query> insertOrUpdate = updatePodRecord(pod, conn);
            conn.batch(insertOrUpdate).execute();
        }
    }

    private List<Query> updatePodRecord(final Pod pod, final DSLContext conn) {
        final List<Query> inserts = new ArrayList<>();
        final List<ResourceRequirements> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .collect(Collectors.toList());
        final long cpuRequest = (long) Utils.resourceRequirementSum(resourceRequirements, "cpu");
        final long memoryRequest = (long) Utils.resourceRequirementSum(resourceRequirements, "memory");
        final long ephemeralStorageRequest =
                (long) Utils.resourceRequirementSum(resourceRequirements, "ephemeral-storage");
        final long podsRequest = 1;

        // The first owner reference is used to break symmetries.
        final List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
        final String ownerName = (owners == null || owners.size() == 0) ? "" : owners.get(0).getName();
        final boolean hasNodeSelector = hasNodeSelector(pod);

//        final boolean hasPodAffinityRequirements;
//        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAffinity() != null) {
//            hasPodAffinityRequirements = pod.getSpec().getAffinity().getPodAffinity()
//                                            .getRequiredDuringSchedulingIgnoredDuringExecution().size() > 0;
//        } else {
//            hasPodAffinityRequirements = false;
//        }
//
//        final boolean hasPodAntiAffinityRequirements;
//        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAntiAffinity() != null) {
//            hasPodAntiAffinityRequirements = pod.getSpec().getAffinity().getPodAntiAffinity()
//                                                .getRequiredDuringSchedulingIgnoredDuringExecution().size() > 0;
//        } else {
//            hasPodAntiAffinityRequirements = false;
//        }

        final int priority = Math.min(pod.getSpec().getPriority() == null ? 10 : pod.getSpec().getPriority(), 100);
        final PodInfo p = Tables.POD_INFO;
        final InsertOnDuplicateSetMoreStep<PodInfoRecord> podInfoInsert = conn.insertInto(Tables.POD_INFO,
                p.POD_NAME,
                p.STATUS,
                p.NODE_NAME,
                p.NAMESPACE,
                p.CPU_REQUEST,
                p.MEMORY_REQUEST,
                p.EPHEMERAL_STORAGE_REQUEST,
                p.PODS_REQUEST,
                p.OWNER_NAME,
                p.CREATION_TIMESTAMP,
                p.HAS_NODE_SELECTOR_LABELS,
                p.HAS_POD_AFFINITY_REQUIREMENTS,
                p.HAS_POD_ANTI_AFFINITY_REQUIREMENTS,
                p.PRIORITY,
                p.SCHEDULERNAME,
                p.EQUIVALENCE_CLASS,
                p.QOS_CLASS)
                .values(pod.getMetadata().getName(),
                        pod.getStatus().getPhase(),
                        pod.getSpec().getNodeName(),
                        pod.getMetadata().getNamespace(),
                        cpuRequest,
                        memoryRequest,
                        ephemeralStorageRequest,
                        podsRequest,
                        ownerName,
                        pod.getMetadata().getCreationTimestamp(),
                        hasNodeSelector,
                        hasAffinityRequirements,
                        hasAntiAffinityRequirements,
                        priority,
                        pod.getSpec().getSchedulerName(),
                        equivalenceClassHash(pod),
                        getQosClass(resourceRequirements).toString()
                )
                .onDuplicateKeyUpdate()
                .set(p.POD_NAME, pod.getMetadata().getName())
                .set(p.STATUS, pod.getStatus().getPhase())
                .set(p.NODE_NAME, pod.getSpec().getNodeName())
                .set(p.NAMESPACE, pod.getMetadata().getNamespace())
                .set(p.CPU_REQUEST, cpuRequest)
                .set(p.MEMORY_REQUEST, memoryRequest)
                .set(p.EPHEMERAL_STORAGE_REQUEST, ephemeralStorageRequest)
                .set(p.PODS_REQUEST, podsRequest)

                // The first owner reference is used to break symmetries.
                .set(p.OWNER_NAME, ownerName)
                .set(p.CREATION_TIMESTAMP, pod.getMetadata().getCreationTimestamp())
                .set(p.HAS_NODE_SELECTOR_LABELS, hasNodeSelector)
                .set(p.HAS_POD_AFFINITY_REQUIREMENTS, hasAffinityRequirements)
                .set(p.HAS_POD_ANTI_AFFINITY_REQUIREMENTS, hasAntiAffinityRequirements)

                // We cap the max priority to 100 to prevent overflow issues in the solver
                .set(p.PRIORITY, priority)

                // This field is important because while we injest info about all pods, we only make scheduling
                // decisions for pods that have dcm-scheduler as their name
                .set(p.SCHEDULERNAME, pod.getSpec().getSchedulerName())

                // Compute equivalent class similar to what the default scheduler does
                .set(p.EQUIVALENCE_CLASS, equivalenceClassHash(pod))

                // QoS classes are defined based on the requests/limits configured for containers in the pod
                .set(p.QOS_CLASS, getQosClass(resourceRequirements).toString());
        inserts.add(podInfoInsert);
        return inserts;
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

    private List<Insert<?>> updateContainerInfoForPod(final Pod pod, final DSLContext conn) {
        final List<Insert<?>> inserts = new ArrayList<>();
        for (final Container container: pod.getSpec().getContainers()) {
            if (container.getPorts() == null || container.getPorts().isEmpty()) {
                continue;
            }
            for (final ContainerPort portInfo: container.getPorts()) {
                // This pod has been assigned to a node already. We therefore update the set of host-ports in
                // use at this node
                if (pod.getSpec().getNodeName() != null && portInfo.getHostPort() != null) {
                    inserts.add(conn.insertInto(Tables.CONTAINER_HOST_PORTS)
                                .values(pod.getSpec().getNodeName(),
                                        portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                        portInfo.getHostPort(),
                                        portInfo.getProtocol()));
                }
                // This pod is yet to be assigned to a host, but it has a hostPort requirement. We record
                // this in the pod_ports_request table
                else if (pod.getStatus().getPhase().equals("") && portInfo.getHostPort() != null) {
                    inserts.add(conn.insertInto(Tables.POD_PORTS_REQUEST)
                                .values(pod.getMetadata().getName(),
                                        portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                        portInfo.getHostPort(),
                                        portInfo.getProtocol()));
                }
            }
            inserts.add(conn.insertInto(Tables.POD_IMAGES).values(pod.getMetadata().getName(), container.getImage()));
        }
        return inserts;
    }

    private List<Insert<PodNodeSelectorLabelsRecord>> updatePodNodeSelectorLabels(final Pod pod,
                                                                                  final DSLContext conn) {
        final List<Insert<PodNodeSelectorLabelsRecord>> podNodeSelectorLabels = new ArrayList<>();
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
                podNodeSelectorLabels.add(
                conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                        .values(pod.getMetadata().getName(), term, matchExpression, numMatchExpressions,
                                labelKey, Operators.In.toString(), labelValue));
            }
        }
        return podNodeSelectorLabels;
    }

    private List<Insert<PodLabelsRecord>> updatePodLabels(final DSLContext conn, final Pod pod) {
        // Update pod_labels table. This will be used for managing affinities, I think?
        final Map<String, String> labels = pod.getMetadata().getLabels();
        if (labels != null) {
            return labels.entrySet().stream().map(
                    (label) -> conn.insertInto(Tables.POD_LABELS)
                         .values(pod.getMetadata().getName(), label.getKey(), label.getValue())
            ).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private List<Insert<PodTolerationsRecord>> updatePodTolerations(final Pod pod, final DSLContext conn) {
        if (pod.getSpec().getTolerations() == null) {
            return Collections.emptyList();
        }
        final List<Insert<PodTolerationsRecord>> inserts = new ArrayList<>();
        for (final Toleration toleration: pod.getSpec().getTolerations()) {
            inserts.add(conn.insertInto(Tables.POD_TOLERATIONS)
                    .values(pod.getMetadata().getName(),
                            toleration.getKey(),
                            toleration.getValue(),
                            toleration.getEffect() == null ? "Equal" : toleration.getEffect(),
                            toleration.getOperator()));
        }
        return Collections.unmodifiableList(inserts);
    }

    private List<Insert<?>> updatePodAffinity(final Pod pod, final DSLContext conn) {
        final List<Insert<?>> inserts = new ArrayList<>();
        final Affinity affinity = pod.getSpec().getAffinity();
        if (affinity == null) {
            return Collections.emptyList();
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
                            inserts.add(
                                conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                                        .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber,
                                                numMatchExpressions, expr.getKey(), expr.getOperator(), value)
                            );
                        }
                    } else {
                        inserts.add(
                            conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                            .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber,
                                    numMatchExpressions, expr.getKey(), expr.getOperator(), null)
                        );
                    }
                }
                termNumber += 1;
            }
        }

        // Pod affinity
        if (affinity.getPodAffinity() != null) {
            affinity.getPodAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().forEach(x ->
                    LOG.debug("Before Affinity: " + x.toString()));
        }
        if (affinity.getPodAntiAffinity() != null) {
            affinity.getPodAntiAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().forEach(x ->
                    LOG.debug("Before AntiAffinity: " + x.toString()));
        }

        final List<PodAffinityTerm> affinityTerms = new ArrayList<>();
        final List<PodAffinityTerm> antiAffinityTerms = new ArrayList<>();

        if (affinity.getPodAffinity() != null) {
            final List<PodAffinityTerm> givenAffinityTerms =
                    affinity.getPodAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            final Iterator<PodAffinityTerm> affinityTermsIterator =
                    givenAffinityTerms.iterator();
             while (affinityTermsIterator.hasNext()) {
                final PodAffinityTerm term = affinityTermsIterator.next();
                if (term.getLabelSelector() != null) {
                    final List<LabelSelectorRequirement> antiAffinityRequirements = new ArrayList<>();
                    final List<LabelSelectorRequirement> requirements = term.getLabelSelector().getMatchExpressions();
                    final Iterator<LabelSelectorRequirement> iterator = requirements.iterator();
                    while (iterator.hasNext()) {
                        final LabelSelectorRequirement requirement = iterator.next();
                        if (requirement.getOperator().equals(Operators.DoesNotExist.toString())) {
                            antiAffinityRequirements.add(new LabelSelectorRequirement(requirement.getKey(),
                                    Operators.DoesNotExist.toString(), requirement.getValues()));
                            iterator.remove();
                        } else if (requirement.getOperator().equals(Operators.NotIn.toString())) {
                            antiAffinityRequirements.add(new LabelSelectorRequirement(requirement.getKey(),
                                    Operators.In.toString(), requirement.getValues()));
                            iterator.remove();
                            affinity.getPodAffinity().getRequiredDuringSchedulingIgnoredDuringExecution()
                                    .forEach(x -> LOG.debug("After removal: " + x.toString()));
                        }
                    }
                    if (term.getLabelSelector().getMatchExpressions().size() == 0) {
                        affinityTermsIterator.remove();
                    }
                        // we removed all conditions, remove this guy as well

                    if (antiAffinityRequirements.size() > 0) { // we found some opposing requirements
                        final LabelSelector selector = new LabelSelector();
                        selector.setMatchExpressions(antiAffinityRequirements);
                        antiAffinityTerms.add(new PodAffinityTerm(selector,
                                term.getNamespaces(), term.getTopologyKey()));
                    }
                }
            }
             if (givenAffinityTerms.size() > 0) {
                 affinityTerms.addAll(givenAffinityTerms);
             }
        }

        if (affinity.getPodAntiAffinity() != null) {
            final List<PodAffinityTerm> givenAffinityTerms =
                    affinity.getPodAntiAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
            final Iterator<PodAffinityTerm> affinityTermsIterator =
                    givenAffinityTerms.iterator();
            while (affinityTermsIterator.hasNext()) {
                final PodAffinityTerm term = affinityTermsIterator.next();
                if (term.getLabelSelector() != null) {
                    final List<LabelSelectorRequirement> affinityRequirements = new ArrayList<>();
                    final List<LabelSelectorRequirement> requirements = term.getLabelSelector().getMatchExpressions();
                    final Iterator<LabelSelectorRequirement> iterator = requirements.iterator();
                    while (iterator.hasNext()) {
                        final LabelSelectorRequirement requirement = iterator.next();
                        if (requirement.getOperator().equals(Operators.DoesNotExist.toString())) {
                            affinityRequirements.add(new LabelSelectorRequirement(requirement.getKey(),
                                    Operators.Exists.toString(), requirement.getValues()));
                            iterator.remove();
                        } else if (requirement.getOperator().equals(Operators.NotIn.toString())) {
                            affinityRequirements.add(new LabelSelectorRequirement(requirement.getKey(),
                                    Operators.In.toString(), requirement.getValues()));
                            iterator.remove();
                        }
                    }

                    if (term.getLabelSelector().getMatchExpressions().size() == 0) {
                        affinityTermsIterator.remove();
                    }

                    if (affinityRequirements.size() > 0) { // we found some opposing requirements
                        final LabelSelector selector = new LabelSelector();
                        selector.setMatchExpressions(affinityRequirements);
                        affinityTerms.add(new PodAffinityTerm(selector,
                                term.getNamespaces(), term.getTopologyKey()));
                    }
                }
            }
            if (givenAffinityTerms.size() > 0) {
                antiAffinityTerms.addAll(givenAffinityTerms);
            }
        }

        if (affinityTerms.size() > 0) {
            affinityTerms.forEach(x -> LOG.debug("After Affinity: " + x.toString()));
        }
        if (antiAffinityTerms.size() > 0) {
            antiAffinityTerms.forEach(x -> LOG.debug("After AntiAffinity: " + x.toString()));
        }

        if (affinityTerms.size() != 0) {
            hasAffinityRequirements = true;
            inserts.addAll(
                    insertPodAffinityTerms(Tables.POD_AFFINITY_MATCH_EXPRESSIONS, pod, affinityTerms));
        }

        if (antiAffinityTerms.size() != 0) {
            hasAntiAffinityRequirements = true;
            inserts.addAll(
                    insertPodAffinityTerms(Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS, pod, antiAffinityTerms));
        }

        return Collections.unmodifiableList(inserts);
    }

    private <T extends Record> List<Insert<?>> insertPodAffinityTerms(final Table<T> table, final Pod pod,
                                                           final List<PodAffinityTerm> terms) {
        final List<Insert<?>> inserts = new ArrayList<>();
        int termNumber = 0;
        for (final PodAffinityTerm term: terms) {
            int matchExpressionNumber = 0;
            final int numMatchExpressions =  term.getLabelSelector().getMatchExpressions().size();
            for (final LabelSelectorRequirement expr: term.getLabelSelector().getMatchExpressions()) {
                matchExpressionNumber += 1;
                try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
                    for (final String value : expr.getValues()) {
                        inserts.add(
                            conn.insertInto(table)
                                .values(pod.getMetadata().getName(), termNumber, matchExpressionNumber,
                                        numMatchExpressions, expr.getKey(), expr.getOperator(), value,
                                        term.getTopologyKey())
                        );
                    }
                }
            }
            termNumber += 1;
        }
        return inserts;
    }

    private long equivalenceClassHash(final Pod pod) {
        return Objects.hash(pod.getMetadata().getNamespace(),
                            pod.getMetadata().getLabels(),
                            pod.getSpec().getAffinity(),
                            pod.getSpec().getInitContainers(),
                            pod.getSpec().getNodeName(),
                            pod.getSpec().getNodeSelector(),
                            pod.getSpec().getTolerations(),
                            pod.getSpec().getVolumes());
    }

    /**
     * Guaranteed -> requests == limits for all containers
     * BestEffort -> no requests nor limits for any containers
     * Burstable -> requests and limits do not match
     */
    private QosClass getQosClass(final List<ResourceRequirements> resourceRequirements) {
        final List<String> supportedResources = List.of("cpu", "memory");
        boolean isGuaranteed = true;
        boolean bestEffort = true;
        for (final ResourceRequirements reqs: resourceRequirements) {
            for (final String supportedResource: supportedResources) {
                final Quantity request = reqs.getRequests() == null ? null : reqs.getRequests().get(supportedResource);
                final Quantity limit = reqs.getLimits() == null ? null : reqs.getLimits().get(supportedResource);
                if (request != null || limit != null) {
                    bestEffort = false;
                }
                if (request == null || !request.equals(limit)) {
                    isGuaranteed = false;
                }
            }
        }

        if (bestEffort) {
            return QosClass.BestEffort;
        }
        if (isGuaranteed) {
            return QosClass.Guaranteed;
        }
        return QosClass.Burstable;
    }

    enum QosClass {
        Guaranteed,
        BestEffort,
        Burstable
    }
}