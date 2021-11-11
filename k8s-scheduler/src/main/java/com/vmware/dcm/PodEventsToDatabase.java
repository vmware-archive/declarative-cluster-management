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
import io.fabric8.kubernetes.api.model.LabelSelector;
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
import org.jooq.DSLContext;
import org.h2.api.Trigger;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vmware.dcm.Utils.convertUnit;


/**
 * Reflects pod events from the Kubernetes API into the database.
 */
class PodEventsToDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(PodEventsToDatabase.class);
    private static final long NEVER_REQUEUED = 0;
    private final IConnectionPool dbConnectionPool;
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

    PodEventsToDatabase(final IConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
    }

    PodEvent handle(final PodEvent event) {
        switch (event.action()) {
            case ADDED -> addPod(event.pod());
            case UPDATED -> updatePod(event.pod(), Objects.requireNonNull(event.oldPod()));
            case DELETED -> deletePod(event.pod());
            default -> throw new IllegalArgumentException(event.toString());
        }
        return event;
    }

    private void addPod(final Pod pod) {
        LOG.info("Adding pod {} (uid: {}, resourceVersion: {})",
                  pod.getMetadata().getName(), pod.getMetadata().getUid(), pod.getMetadata().getResourceVersion());
        if (pod.getMetadata().getUid() != null &&
            deletedUids.getIfPresent(pod.getMetadata().getUid()) != null) {
            LOG.trace("Received stale event for pod that we already deleted: {} (uid: {}, resourceVersion {}). " +
                      "Ignoring", pod.getMetadata().getName(), pod.getMetadata().getUid(),
                      pod.getMetadata().getResourceVersion());
            return;
        }
        final long start = System.nanoTime();
        try (final DSLContext conn = dbConnectionPool.getConnectionToDb()) {
            final List<Query> inserts = new ArrayList<>();
            inserts.addAll(updatePodRecord(pod, conn));
            inserts.addAll(updateContainerInfoForPod(pod, conn));
            inserts.addAll(updatePodLabels(pod, conn));
            // updateVolumeInfoForPod(pod, pvcToPv, conn);
            inserts.addAll(updatePodTolerations(pod, conn));
            inserts.addAll(updatePodAffinity(pod, conn));
            inserts.addAll(updateResourceRequests(pod, conn));
            inserts.addAll(updatePodTopologySpread(pod, conn));
            conn.batch(inserts).execute();
        }
        final long end = System.nanoTime();
        LOG.info("{} pod added in {}ns", pod.getMetadata().getName(), (end - start));
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
                .where(DSL.field(Tables.POD_INFO.UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        }
    }

    private void updatePod(final Pod pod, final Pod oldPod) {
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
            if (!Objects.equals(pod.getSpec().getContainers(), oldPod.getSpec().getContainers())) {
                insertOrUpdate.addAll(updateContainerInfoForPod(pod, conn));
                insertOrUpdate.addAll(updateResourceRequests(pod, conn));
            }
            if (!Objects.equals(pod.getMetadata().getLabels(), oldPod.getMetadata().getLabels())) {
                insertOrUpdate.addAll(updatePodLabels(pod, conn));
            }
            if (!Objects.equals(pod.getSpec().getTolerations(), oldPod.getSpec().getTolerations())) {
                insertOrUpdate.addAll(updatePodTolerations(pod, conn));
            }
            if (!Objects.equals(pod.getSpec().getAffinity(), oldPod.getSpec().getAffinity())) {
                insertOrUpdate.addAll(updatePodAffinity(pod, conn));
            }
            if (!Objects.equals(pod.getSpec().getTopologySpreadConstraints(),
                                oldPod.getSpec().getTopologySpreadConstraints())) {
                insertOrUpdate.addAll(updatePodTopologySpread(pod, conn));
            }
            conn.batch(insertOrUpdate).execute();
        }
    }

    private List<Query> updatePodRecord(final Pod pod, final DSLContext conn) {
        final List<Query> inserts = new ArrayList<>();
        final List<ResourceRequirements> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .collect(Collectors.toList());

        // The first owner reference is used to break symmetries.
        final List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
        final String ownerName = (owners == null || owners.size() == 0) ? "" : owners.get(0).getName();
        final boolean hasNodeSelector = hasNodeSelector(pod);

        boolean hasPodAffinityRequirements = false;
        boolean hasPodAntiAffinityRequirements = false;
        boolean hasNodePortRequirements = false;
        boolean hasPodTopologySpreadConstraints = false;

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
        if (pod.getSpec().getTopologySpreadConstraints() != null
                && !pod.getSpec().getTopologySpreadConstraints().isEmpty()) {
            hasPodTopologySpreadConstraints = true;
        }

        final int priority = Math.min(pod.getSpec().getPriority() == null ? 10 : pod.getSpec().getPriority(), 100);
        final PodInfo p = Tables.POD_INFO;
        final long resourceVersion = Long.parseLong(pod.getMetadata().getResourceVersion());
        LOG.info("Insert/Update pod {}, {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
                pod.getMetadata().getUid(),
                pod.getMetadata().getName(),
                pod.getStatus().getPhase(),
                pod.getSpec().getNodeName(),
                pod.getMetadata().getNamespace(),
                ownerName,
                pod.getMetadata().getCreationTimestamp(),
                hasNodeSelector,
                hasPodAffinityRequirements,
                hasPodAntiAffinityRequirements,
                hasNodePortRequirements,
                hasPodTopologySpreadConstraints,
                priority,
                pod.getSpec().getSchedulerName(),
                equivalenceClassHash(pod),
                getQosClass(resourceRequirements),
                resourceVersion);
        // In order for this insert to work for the DDlog backend, we MUST ensure the columns are ordered exactly
        // as they are ordered in the table creation SQL statement.
        final InsertOnDuplicateStep<PodInfoRecord> podInfoInsert = conn.insertInto(Tables.POD_INFO,
                p.UID,
                p.POD_NAME,
                p.STATUS,
                p.NODE_NAME,
                p.NAMESPACE,
                p.OWNER_NAME,
                p.CREATION_TIMESTAMP,
                p.PRIORITY,
                p.SCHEDULERNAME,
                p.HAS_NODE_SELECTOR_LABELS,
                p.HAS_POD_AFFINITY_REQUIREMENTS,
                p.HAS_POD_ANTI_AFFINITY_REQUIREMENTS,
                p.HAS_NODE_PORT_REQUIREMENTS,
                p.HAS_TOPOLOGY_SPREAD_CONSTRAINTS,
                p.EQUIVALENCE_CLASS,
                p.QOS_CLASS,
                p.RESOURCEVERSION,
                p.LAST_REQUEUE)
                .values(pod.getMetadata().getUid(),
                        pod.getMetadata().getName(),
                        pod.getStatus().getPhase(),
                        pod.getSpec().getNodeName(),
                        pod.getMetadata().getNamespace(),
                        ownerName,
                        pod.getMetadata().getCreationTimestamp(),
                        priority,
                        pod.getSpec().getSchedulerName(),
                        hasNodeSelector,
                        hasPodAffinityRequirements,
                        hasPodAntiAffinityRequirements,
                        hasNodePortRequirements,
                        hasPodTopologySpreadConstraints,
                        equivalenceClassHash(pod),
                        getQosClass(resourceRequirements).toString(),
                        resourceVersion,
                        NEVER_REQUEUED
                );

        /*
         * TODO: InsertOnDuplicateSetMoreStep generates a `merge` SQL statement, which isn't currently handled by the
         * SQl->DDlog translator. For now we comment, but need to address duplicate keys later.
         */
        /*final InsertOnDuplicateSetMoreStep<PodInfoRecord> podInfoInsert = conn.insertInto(Tables.POD_INFO,
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
                p.PRIORITY,
                p.SCHEDULERNAME,
                p.HAS_NODE_SELECTOR_LABELS,
                p.HAS_POD_AFFINITY_REQUIREMENTS,
                p.HAS_POD_ANTI_AFFINITY_REQUIREMENTS,
                p.HAS_NODE_PORT_REQUIREMENTS,
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
                        priority,
                        pod.getSpec().getSchedulerName(),
                        hasNodeSelector,
                        hasPodAffinityRequirements,
                        hasPodAntiAffinityRequirements,
                        hasNodePortRequirements,
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

                // The first owner reference is used to break symmetries.
                .set(p.OWNER_NAME, ownerName)
                .set(p.CREATION_TIMESTAMP, pod.getMetadata().getCreationTimestamp())
                .set(p.HAS_NODE_SELECTOR_LABELS, hasNodeSelector)
                .set(p.HAS_POD_AFFINITY_REQUIREMENTS, hasPodAffinityRequirements)
                .set(p.HAS_POD_ANTI_AFFINITY_REQUIREMENTS, hasPodAntiAffinityRequirements)
                .set(p.HAS_NODE_PORT_REQUIREMENTS, hasNodePortRequirements)
                .set(p.HAS_TOPOLOGY_SPREAD_CONSTRAINTS, hasPodTopologySpreadConstraints)

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
                .set(p.RESOURCEVERSION, resourceVersion);*/
        inserts.add(podInfoInsert);
        return inserts;
    }

    private List<Insert<?>> updateResourceRequests(final Pod pod, final DSLContext conn) {
        conn.deleteFrom(Tables.POD_RESOURCE_DEMANDS)
                .where(DSL.field(Tables.POD_RESOURCE_DEMANDS.UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        final List<Insert<?>> inserts = new ArrayList<>();
        final Map<String, Long> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .filter(Objects::nonNull)
                .map(ResourceRequirements::getRequests)
                .filter(Objects::nonNull)
                .flatMap(e -> e.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey,
                                          es -> convertUnit(es.getValue(), es.getKey()),
                                          Long::sum));
        final Map<String, Long> overheads = pod.getSpec().getOverhead() != null ?
                pod.getSpec().getOverhead().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                                  es -> convertUnit(es.getValue(), es.getKey()),
                                                  Long::sum)) : new HashMap<>();
        resourceRequirements.putIfAbsent("pods", 1L);
        resourceRequirements.forEach((resource, demand) -> inserts.add(
                conn.insertInto(Tables.POD_RESOURCE_DEMANDS)
                    .values(pod.getMetadata().getUid(), resource, demand + overheads.getOrDefault(resource, 0L))
        ));
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
        conn.deleteFrom(Tables.POD_IMAGES)
                .where(DSL.field(Tables.POD_IMAGES.POD_UID.getUnqualifiedName()).eq(pod.getMetadata().getUid()))
                .execute();
        conn.deleteFrom(Tables.POD_PORTS_REQUEST)
                .where(DSL.field(Tables.POD_PORTS_REQUEST.POD_UID.getUnqualifiedName()).eq(pod.getMetadata().getUid()))
                .execute();

        // Record all unique images in the container
        pod.getSpec().getContainers().stream()
                .map(Container::getImage)
                .distinct()
                .forEach(image ->
                    inserts.add(conn.insertInto(Tables.POD_IMAGES).values(pod.getMetadata().getUid(), image))
                );

        for (final Container container: pod.getSpec().getContainers()) {
            if (container.getPorts() == null || container.getPorts().isEmpty()) {
                continue;
            }
            for (final ContainerPort portInfo: container.getPorts()) {
                if (portInfo.getHostPort() == null) {
                    continue;
                }
                inserts.add(conn.insertInto(Tables.POD_PORTS_REQUEST)
                            .values(pod.getMetadata().getUid(),
                                    portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                    portInfo.getHostPort(),
                                    portInfo.getProtocol()));
            }
        }
        return inserts;
    }

    private List<Insert<PodLabelsRecord>> updatePodLabels(final Pod pod, final DSLContext conn) {
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
        conn.deleteFrom(Tables.POD_TOLERATIONS)
            .where(DSL.field(Tables.POD_TOLERATIONS.POD_UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        final List<Insert<PodTolerationsRecord>> inserts = new ArrayList<>();
        for (final Toleration toleration: pod.getSpec().getTolerations()) {
            inserts.add(conn.insertInto(Tables.POD_TOLERATIONS)
                    .values(pod.getMetadata().getUid(),
                            toleration.getKey() == null ? "" : toleration.getKey(),
                            toleration.getValue() == null ? "" : toleration.getValue(),
                            toleration.getEffect() == null ? "" : toleration.getEffect(),
                            toleration.getOperator() == null ? "Equal" : toleration.getOperator()));
        }
        return Collections.unmodifiableList(inserts);
    }

    private List<Insert<?>> updatePodAffinity(final Pod pod, final DSLContext conn) {
        final List<Insert<?>> inserts = new ArrayList<>();
        final Affinity affinity = pod.getSpec().getAffinity();
        conn.deleteFrom(Tables.POD_NODE_SELECTOR_LABELS)
                .where(DSL.field(Tables.POD_NODE_SELECTOR_LABELS.POD_UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        conn.deleteFrom(Tables.POD_AFFINITY_MATCH_EXPRESSIONS)
                .where(DSL.field(Tables.POD_AFFINITY_MATCH_EXPRESSIONS.POD_UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        conn.deleteFrom(Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS)
                .where(DSL.field(Tables.POD_ANTI_AFFINITY_MATCH_EXPRESSIONS.POD_UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();

        // also handled using the same POD_NODE_SELECTOR_LABELS table
        inserts.addAll(updatePodNodeSelectorLabels(pod, conn));
        Optional.ofNullable(affinity)
                .map(Affinity::getNodeAffinity)
                .map(NodeAffinity::getRequiredDuringSchedulingIgnoredDuringExecution)
                .ifPresent(selector -> {
                    final AtomicInteger termNumber = new AtomicInteger(0);
                    selector.getNodeSelectorTerms().forEach(term -> {
                            final Object[] exprIds = term.getMatchExpressions().stream()
                                    .map(expr -> toMatchExpressionId(conn, expr.getKey(), expr.getOperator(),
                                                                     expr.getValues())).toList().toArray(new Long[0]);
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

    private List<Insert<PodNodeSelectorLabelsRecord>> updatePodNodeSelectorLabels(final Pod pod,
                                                                                  final DSLContext conn) {
        final List<Insert<PodNodeSelectorLabelsRecord>> podNodeSelectorLabels = new ArrayList<>();
        // Update pod_node_selector_labels table
        final Map<String, String> nodeSelector = pod.getSpec().getNodeSelector();
        if (nodeSelector != null) {
            // Using a node selector is equivalent to having a single node-selector term and a list of match expressions
            final int term = 0;
            final Long[] matchExpressions = nodeSelector.entrySet().stream()
                    .map(e -> toMatchExpressionId(conn, e.getKey(), Operators.In.toString(),
                            List.of(e.getValue())))
                    .collect(Collectors.toList()).toArray(new Long[0]);
            if (matchExpressions.length == 0) {
                return podNodeSelectorLabels;
            }
            podNodeSelectorLabels.add(conn.insertInto(Tables.POD_NODE_SELECTOR_LABELS)
                    .values(pod.getMetadata().getUid(), term, matchExpressions));
        }
        return podNodeSelectorLabels;
    }

    private Object[] selectorToMatchExpressions(final DSLContext conn, final LabelSelector selector) {
        final Stream<Long> matchLabels = selector.getMatchLabels() == null ? Stream.empty() :
                selector.getMatchLabels().entrySet().stream()
                .map(e -> toMatchExpressionId(conn, e.getKey(), Operators.In.toString(), List.of(e.getValue())));
        final Stream<Long> matchExpressions = selector.getMatchExpressions() == null ? Stream.empty() :
                selector.getMatchExpressions().stream()
                .map(expr -> toMatchExpressionId(conn, expr.getKey(), expr.getOperator(),
                        expr.getValues()));
        return Stream.concat(matchLabels, matchExpressions).toArray();
    }

    private List<Insert<?>> insertPodAffinityTerms(final Table<?> table, final Pod pod,
                                                   final List<PodAffinityTerm> terms, final DSLContext conn) {
        final List<Insert<?>> inserts = new ArrayList<>();
        int termNumber = 0;
        for (final PodAffinityTerm term: terms) {
            final Object[] matchExpressions = term.getLabelSelector().getMatchExpressions().stream()
                    .map(e -> toMatchExpressionId(conn, e.getKey(), e.getOperator(), e.getValues()))
                    .toList().toArray(new Long[0]);
            inserts.add(conn.insertInto(table)
                            .values(pod.getMetadata().getUid(), termNumber, matchExpressions, term.getTopologyKey()));
            termNumber += 1;
        }
        return inserts;
    }

    private List<Insert<?>> updatePodTopologySpread(final Pod pod, final DSLContext conn) {
        conn.deleteFrom(Tables.POD_TOPOLOGY_SPREAD_CONSTRAINTS)
            .where(DSL.field(Tables.POD_TOPOLOGY_SPREAD_CONSTRAINTS.UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        if (pod.getSpec().getTopologySpreadConstraints() == null) {
            return Collections.emptyList();
        }
        final List<Insert<?>> inserts = new ArrayList<>();
        pod.getSpec().getTopologySpreadConstraints().forEach(c -> {
            final Object[] matchedIds = selectorToMatchExpressions(conn, c.getLabelSelector());
            inserts.add(conn.insertInto(Tables.POD_TOPOLOGY_SPREAD_CONSTRAINTS)
                    .values(pod.getMetadata().getUid(), c.getMaxSkew(), c.getWhenUnsatisfiable(),
                            c.getTopologyKey(), matchedIds));
        });
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
            final MatchExpressionsRecord record =
                    conn.selectFrom(me)
                    .where(DSL.field(me.LABEL_KEY.getUnqualifiedName()).eq(key)
                            .and(DSL.field(me.LABEL_OPERATOR.getUnqualifiedName()).eq(operator))
                            .and(DSL.field(me.LABEL_VALUES.getUnqualifiedName()).eq(valuesArray)))
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
