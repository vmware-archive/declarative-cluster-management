/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.MatchExpressions;
import com.vmware.dcm.k8s.generated.tables.PodInfo;
import com.vmware.dcm.k8s.generated.tables.records.MatchExpressionsRecord;
import com.vmware.dcm.k8s.generated.tables.records.PodInfoRecord;
import com.vmware.dcm.k8s.generated.tables.records.PodLabelsRecord;
import com.vmware.dcm.k8s.generated.tables.records.PodNodeSelectorLabelsRecord;
import com.vmware.dcm.k8s.generated.tables.records.PodTolerationsRecord;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinity;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodAntiAffinity;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import org.h2.api.Trigger;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.Query;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * Reflects pod events from the Kubernetes API into the database.
 */
class PodEventsToDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(PodEventsToDatabase.class);
    private static final long NEVER_REQUEUED = 0;
    private final DBConnectionPool dbConnectionPool;
    private final Cache<String, Boolean> deletedUids = CacheBuilder.newBuilder()
                                                                      .expireAfterWrite(5, TimeUnit.MINUTES)
                                                                      .build();
    private final AtomicLong expressionIds = new AtomicLong();

    private enum Operators {
        In,
        Exists,
        NotIn,
        DoesNotExist
    }

    PodEventsToDatabase(final DBConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            conn.execute("create or replace trigger nodeInfoResourceUpdateOnPodInsert " +
                         "after insert on " + Tables.POD_INFO.getName() + " for each row " +
                         "call \"" + NodeInfoIncrementalUpdate.class.getName() + "\"");
            conn.execute("create or replace trigger nodeInfoResourceUpdateOnPodUpdate " +
                         "after update on " + Tables.POD_INFO.getName() + " for each row " +
                         "call \"" + NodeInfoIncrementalUpdate.class.getName() + "\"");
            conn.execute("create or replace trigger nodeInfoResourceUpdateOnPodDelete " +
                          "after delete on " + Tables.POD_INFO.getName() + " for each row " +
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
                    "update " + Tables.NODE_INFO.getName() +
                    " set cpu_allocated = cpu_allocated + ?," +
                     "memory_allocated = memory_allocated + ?," +
                     "ephemeral_storage_allocated = ephemeral_storage_allocated + ?," +
                     "pods_allocated = pods_allocated + ? " +
                     "where name = ?")) {
                final boolean isInsert = (oldRow == null && newRow != null && newRow[3] != null);
                final boolean isDeletion = (oldRow != null && newRow == null && oldRow[3] != null);
                final boolean isNodeNameUpdate =
                        oldRow != null && newRow != null && oldRow[3] == null && newRow[3] != null;
                if (isDeletion) {
                    updateNodeAgainstPodInfo(stmt, oldRow, (String) oldRow[3], true);
                } else if (isInsert || isNodeNameUpdate) {
                    updateNodeAgainstPodInfo(stmt, newRow, (String) newRow[3], false);
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void remove() {
        }

        private void updateNodeAgainstPodInfo(final PreparedStatement statement, final Object[] podRow,
                                              final String nodeName, final boolean isDeletion)
                                              throws SQLException {
            final int sign = isDeletion ? -1 : 1;
            statement.setLong(1, sign * ((long) podRow[5])); // CPU_REQUEST
            statement.setLong(2, sign * ((long) podRow[6])); // MEMORY_REQUEST
            statement.setLong(3, sign * ((long) podRow[7])); // EPHEMERAL_STORAGE_REQUEST
            statement.setLong(4, sign * ((long) podRow[8])); // PODS_REQUEST
            statement.setString(5, nodeName);
            statement.execute();
        }
    }

    PodEvent handle(final PodEvent event) {
        switch (event.action()) {
            case ADDED -> addPod(event.pod());
            case UPDATED -> updatePod(event.pod());
            case DELETED -> deletePod(event.pod());
            default -> throw new IllegalArgumentException(event.toString());
        }
        return event;
    }

    private void addPod(final Pod pod) {
        LOG.trace("Adding pod {} (uid: {}, resourceVersion: {})",
                  pod.getMetadata().getName(), pod.getMetadata().getUid(), pod.getMetadata().getResourceVersion());
        if (pod.getMetadata().getUid() != null &&
            deletedUids.getIfPresent(pod.getMetadata().getUid()) != null) {
            LOG.trace("Received stale event for pod that we already deleted: {} (uid: {}, resourceVersion {}). " +
                      "Ignoring", pod.getMetadata().getName(), pod.getMetadata().getUid(),
                      pod.getMetadata().getResourceVersion());
            return;
        }
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> inserts = new ArrayList<>();
            inserts.addAll(updatePodRecord(pod, conn));
            inserts.addAll(updateContainerInfoForPod(pod, conn));
            inserts.addAll(updatePodNodeSelectorLabels(pod, conn));
            inserts.addAll(updatePodLabels(conn, pod));
            // updateVolumeInfoForPod(pod, pvcToPv, conn);
            inserts.addAll(updatePodTolerations(pod, conn));
            inserts.addAll(updatePodAffinity(pod, conn));
            conn.batch(inserts).execute();
        }
    }

    private void deletePod(final Pod pod) {
        LOG.trace("Deleting pod {} (uid: {}, resourceVersion: {})",
                  pod.getMetadata().getName(), pod.getMetadata().getUid(), pod.getMetadata().getResourceVersion());
        // The assumption here is that all foreign key references to pod_info.pod_name will be deleted using
        // a delete cascade
        if (pod.getMetadata().getUid() != null &&
                deletedUids.getIfPresent(pod.getMetadata().getUid()) == null) {
            deletedUids.put(pod.getMetadata().getUid(), true);
        }
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            conn.deleteFrom(Tables.POD_INFO)
                .where(Tables.POD_INFO.UID.eq(pod.getMetadata().getUid())).execute();
        }
    }

    private void updatePod(final Pod pod) {
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final PodInfoRecord existingPodInfoRecord = conn.selectFrom(Tables.POD_INFO)
                    .where(Tables.POD_INFO.UID.eq(pod.getMetadata().getUid()))
                    .fetchOne();
            if (existingPodInfoRecord == null) {
                LOG.trace("Pod {} (uid: {}) does not exist. Skipping",
                          pod.getMetadata().getName(), pod.getMetadata().getUid());
                return;
            }
            final long incomingResourceVersion = Long.parseLong(pod.getMetadata().getResourceVersion());
            if (existingPodInfoRecord.getResourceversion() >= incomingResourceVersion) {
                LOG.trace("Received a stale pod event {} (uid: {}, resourceVersion: {}). Ignoring",
                          pod.getMetadata().getName(), pod.getMetadata().getUid(),
                          pod.getMetadata().getResourceVersion());
                return;
            }
            if (pod.getSpec().getNodeName() == null &&
                existingPodInfoRecord.getNodeName() != null) {
                LOG.trace("Received a duplicate event for a node that we have already scheduled (old: {}, new:{}). " +
                         "Ignoring.", existingPodInfoRecord.getNodeName(), pod.getSpec().getNodeName());
                return;
            }
            if (pod.getMetadata().getUid() != null &&
                    deletedUids.getIfPresent(pod.getMetadata().getUid()) != null) {
                LOG.trace("Received stale event for pod that we already deleted: {} (uid: {}, resourceVersion: {}). " +
                          "Ignoring", pod.getMetadata().getName(), pod.getMetadata().getUid(),
                          pod.getMetadata().getResourceVersion());
                return;
            }
            LOG.trace("Updating pod {} (uid: {}, resourceVersion: {})", pod.getMetadata().getName(),
                      pod.getMetadata().getUid(), pod.getMetadata().getResourceVersion());
            final List<Query> insertOrUpdate = updatePodRecord(pod, conn);
            conn.batch(insertOrUpdate).execute();
        }
    }

    private List<Query> updatePodRecord(final Pod pod, final DSLContext conn) {
        final List<Query> inserts = new ArrayList<>();
        final List<ResourceRequirements> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .collect(Collectors.toList());
        final long cpuRequest = Utils.resourceRequirementSum(resourceRequirements, "cpu");
        final long memoryRequest = Utils.resourceRequirementSum(resourceRequirements, "memory");
        final long ephemeralStorageRequest = Utils.resourceRequirementSum(resourceRequirements, "ephemeral-storage");
        final long podsRequest = 1;

        // The first owner reference is used to break symmetries.
        final List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
        final String ownerName = (owners == null || owners.size() == 0) ? "" : owners.get(0).getName();
        final boolean hasNodeSelector = hasNodeSelector(pod);

        boolean hasPodAffinityRequirements = false;
        boolean hasPodAntiAffinityRequirements = false;
        boolean hasNodePortRequirements = false;

        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAffinity() != null) {
            hasPodAffinityRequirements = true;
        }

        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAntiAffinity() != null) {
            hasPodAntiAffinityRequirements = true;
        }

        if (pod.getSpec().getContainers() != null
             && pod.getSpec().getContainers().stream()
                .filter(c -> c.getPorts() != null).flatMap(c -> c.getPorts().stream())
                .anyMatch(containerPort -> containerPort.getHostPort() != null)) {
            hasNodePortRequirements = true;
        }

        final int priority = Math.min(pod.getSpec().getPriority() == null ? 10 : pod.getSpec().getPriority(), 100);
        final PodInfo p = Tables.POD_INFO;
        final long resourceVersion = Long.parseLong(pod.getMetadata().getResourceVersion());
        LOG.trace("Insert/Update pod {}, {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
                pod.getMetadata().getUid(),
                pod.getMetadata().getName(),
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
                hasPodAffinityRequirements,
                hasPodAntiAffinityRequirements,
                hasNodePortRequirements,
                priority,
                pod.getSpec().getSchedulerName(),
                equivalenceClassHash(pod),
                getQosClass(resourceRequirements),
                resourceVersion);
        final InsertOnDuplicateSetMoreStep<PodInfoRecord> podInfoInsert = conn.insertInto(Tables.POD_INFO,
                p.UID,
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
                p.HAS_NODE_PORT_REQUIREMENTS,
                p.PRIORITY,
                p.SCHEDULERNAME,
                p.EQUIVALENCE_CLASS,
                p.QOS_CLASS,
                p.RESOURCEVERSION,
                p.LAST_REQUEUE)
                .values(pod.getMetadata().getUid(),
                        pod.getMetadata().getName(),
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
                        hasPodAffinityRequirements,
                        hasPodAntiAffinityRequirements,
                        hasNodePortRequirements,
                        priority,
                        pod.getSpec().getSchedulerName(),
                        equivalenceClassHash(pod),
                        getQosClass(resourceRequirements).toString(),
                        resourceVersion,
                        NEVER_REQUEUED
                )
                .onDuplicateKeyUpdate()
                .set(p.UID, pod.getMetadata().getUid())
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
                .set(p.HAS_POD_AFFINITY_REQUIREMENTS, hasPodAffinityRequirements)
                .set(p.HAS_POD_ANTI_AFFINITY_REQUIREMENTS, hasPodAntiAffinityRequirements)
                .set(p.HAS_NODE_PORT_REQUIREMENTS, hasNodePortRequirements)

                // We cap the max priority to 100 to prevent overflow issues in the solver
                .set(p.PRIORITY, priority)

                // This field is important because while we injest info about all pods, we only make scheduling
                // decisions for pods that have dcm-scheduler as their name
                .set(p.SCHEDULERNAME, pod.getSpec().getSchedulerName())

                // Compute equivalent class similar to what the default scheduler does
                .set(p.EQUIVALENCE_CLASS, equivalenceClassHash(pod))

                // QoS classes are defined based on the requests/limits configured for containers in the pod
                .set(p.QOS_CLASS, getQosClass(resourceRequirements).toString())

                // This should monotonically increase
                .set(p.RESOURCEVERSION, resourceVersion);
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
                inserts.add(conn.insertInto(Tables.POD_PORTS_REQUEST)
                            .values(pod.getMetadata().getUid(),
                                    portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                    portInfo.getHostPort(),
                                    portInfo.getProtocol()));
            }
            inserts.add(conn.insertInto(Tables.POD_IMAGES).values(pod.getMetadata().getUid(), container.getImage()));
        }
        return inserts;
    }

    private List<Insert<PodNodeSelectorLabelsRecord>> updatePodNodeSelectorLabels(final Pod pod,
                                                                                  final DSLContext conn) {
        final List<Insert<PodNodeSelectorLabelsRecord>> podNodeSelectorLabels = new ArrayList<>();
        // Update pod_node_selector_labels table
        final Map<String, String> nodeSelector = pod.getSpec().getNodeSelector();
        if (nodeSelector != null) {
            // Using a node selector is equivalent to having a single node-selector term and a list of match expressions
            final int term = 0;
            final Object[] matchExpressions = nodeSelector.entrySet().stream()
                                                .map(e -> toMatchExpressionId(conn, e.getKey(), Operators.In.toString(),
                                                                              List.of(e.getValue())))
                                                .toArray();
            if (matchExpressions.length == 0) {
                return podNodeSelectorLabels;
            }
            podNodeSelectorLabels.add(conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                                          .values(pod.getMetadata().getUid(), term, matchExpressions));
        }
        return podNodeSelectorLabels;
    }

    private List<Insert<PodLabelsRecord>> updatePodLabels(final DSLContext conn, final Pod pod) {
        // Update pod_labels table. This will be used for managing affinities, I think?
        final Map<String, String> labels = pod.getMetadata().getLabels();
        if (labels != null) {
            return labels.entrySet().stream().map(
                    (label) -> conn.insertInto(Tables.POD_LABELS)
                         .values(pod.getMetadata().getUid(), label.getKey(), label.getValue())
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
                    .values(pod.getMetadata().getUid(),
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
        Optional.ofNullable(affinity)
                .map(Affinity::getNodeAffinity)
                .map(NodeAffinity::getRequiredDuringSchedulingIgnoredDuringExecution)
                .ifPresent(selector -> {
                    final AtomicInteger termNumber = new AtomicInteger(0);
                    selector.getNodeSelectorTerms().forEach(term -> {
                            final Object[] exprIds = term.getMatchExpressions().stream()
                                    .map(expr -> toMatchExpressionId(conn, expr.getKey(), expr.getOperator(),
                                                                     expr.getValues())).toArray();
                            inserts.add(conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                                            .values(pod.getMetadata().getUid(), termNumber, exprIds));
                            termNumber.incrementAndGet();
                        }
                    );
                });

        // Pod affinity
        Optional.ofNullable(affinity)
                .map(Affinity::getPodAffinity)
                .map(PodAffinity::getRequiredDuringSchedulingIgnoredDuringExecution)
                .ifPresent(podAffinityTerm -> inserts.addAll(
                    insertPodAffinityTerms(Tables.POD_AFFINITY_MATCH_EXPRESSIONS, pod, podAffinityTerm, conn)));

        // Pod Anti affinity
        Optional.ofNullable(affinity)
                .map(Affinity::getPodAntiAffinity)
                .map(PodAntiAffinity::getRequiredDuringSchedulingIgnoredDuringExecution)
                .ifPresent(podAntiAffinityTerm -> inserts.addAll(
                    insertPodAffinityTerms(Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS, pod, podAntiAffinityTerm,
                                           conn)));
        return Collections.unmodifiableList(inserts);
    }

    private List<Insert<?>> insertPodAffinityTerms(final Table<?> table, final Pod pod,
                                                   final List<PodAffinityTerm> terms, final DSLContext conn) {
        final List<Insert<?>> inserts = new ArrayList<>();
        int termNumber = 0;
        for (final PodAffinityTerm term: terms) {
            final Object[] matchExpressions = term.getLabelSelector().getMatchExpressions().stream()
                    .map(e -> toMatchExpressionId(conn, e.getKey(), e.getOperator(), e.getValues()))
                    .toArray();
            inserts.add(conn.insertInto(table)
                            .values(pod.getMetadata().getUid(), termNumber, matchExpressions, term.getTopologyKey()));
            termNumber += 1;
        }
        return inserts;
    }

    private long toMatchExpressionId(final DSLContext conn, final String key, final String operator,
                                     @Nullable final List<String> values) {
        final MatchExpressions me = Tables.MATCH_EXPRESSIONS;
        final Object[] valuesArray = values == null ? new Object[0] : values.toArray();
        synchronized (this) {
            // Ideally, we'd use an auto-incrementing field on the expression_id column to handle this
            // in a single insert/returning statement. But we keep the ID incrementing outside the database
            // in anticipation of using ddlog, which does not yet support auto-incrementing IDs.
            final MatchExpressionsRecord record = conn.selectFrom(me)
                    .where(me.LABEL_KEY.eq(key)
                            .and(me.LABEL_OPERATOR.eq(operator))
                            .and(me.LABEL_VALUES.eq(valuesArray)))
                    .fetchOne();
            if (record == null) {
                final MatchExpressionsRecord newRecord = conn.newRecord(me);
                final long value = expressionIds.incrementAndGet();
                newRecord.setExprId(value);
                newRecord.setLabelKey(key);
                newRecord.setLabelOperator(operator);
                newRecord.setLabelValues(valuesArray);
                newRecord.store();
                return value;
            } else {
                return record.getExprId();
            }
        }
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