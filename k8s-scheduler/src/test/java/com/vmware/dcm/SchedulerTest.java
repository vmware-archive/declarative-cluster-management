/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.k8s.generated.Tables;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeSelector;
import io.fabric8.kubernetes.api.model.NodeSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinity;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodAffinityTermBuilder;
import io.fabric8.kubernetes.api.model.PodAntiAffinity;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.CartesianProductTest;
import org.junitpioneer.jupiter.CartesianValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vmware.dcm.TestScenario.Op.COLOCATED_WITH;
import static com.vmware.dcm.TestScenario.Op.EQUALS;
import static com.vmware.dcm.TestScenario.Op.IN;
import static com.vmware.dcm.TestScenario.Op.NOT_COLOCATED_WITH;
import static com.vmware.dcm.TestScenario.Op.NOT_EQUALS;
import static com.vmware.dcm.TestScenario.Op.NOT_IN;
import static com.vmware.dcm.TestScenario.nodeGroup;
import static com.vmware.dcm.TestScenario.nodesForPodGroup;
import static com.vmware.dcm.TestScenario.podGroup;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.jooq.impl.DSL.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;




/**
 * Tests for the scheduler
 */
@ExtendWith({})
@SuppressWarnings("unchecked")
public class SchedulerTest {
     record TestArguments(List<Object> args) {
        public Object get(final int index) {
            return args.get(index);
        }

        @Override
        public String toString() {
            return (String) args.get(0);
        }
    }

    @BeforeAll
    @SuppressWarnings("all")
    public static void compileDDlog() {
         DDlogDBConnectionPool.create(null, true);
    }

    @SuppressWarnings("all")
    public static DDlogDBConnectionPool setupDDlog() {
        return DDlogDBConnectionPool.create(null, false);
    }

    /*
     * Test if multiple connections from our connection pool see each other's changes
     */
    @Test
    public void testMultipleConnections() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn1 = dbConnectionPool.getConnectionToDb();
        conn1.insertInto(Tables.NODE_INFO).values("xyz", "xyz", true, true, true, true, true, true, true)
              .execute();
        assertEquals(1, conn1.fetchCount(Tables.NODE_INFO));

        final DSLContext connNew = dbConnectionPool.getConnectionToDb();
        assertEquals(1, connNew.fetchCount(Tables.NODE_INFO));
    }

    /*
     * Double checks that delete cascades work.
     */
    @Test
    public void testDeleteCascade() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final PodEventsToDatabase handler = new PodEventsToDatabase(dbConnectionPool);
        final String podName = "p1";
        final Pod pod = newPod(podName, UUID.randomUUID(), "Pending", Collections.emptyMap(),
                Collections.singletonMap("k", "v"));
        handler.handle(new PodEvent(PodEvent.Action.ADDED, pod));
        assertTrue(conn.fetchExists(Tables.POD_INFO));
        assertTrue(conn.fetchExists(Tables.POD_LABELS));
        handler.handle(new PodEvent(PodEvent.Action.DELETED, pod));
        assertFalse(conn.fetchExists(Tables.POD_INFO));
        assertFalse(conn.fetchExists(Tables.POD_LABELS));
    }

    /*
     * Test pod insert, update and delete paths
     */
    @Test
    public void testPodToDbOperations() {
        final IConnectionPool pool = setupDDlog();
        final DSLContext conn = pool.getConnectionToDb();
        final PodEventsToDatabase handler = new PodEventsToDatabase(pool);
        final String podName = "p1";
        final Pod pod = newPod(podName, UUID.randomUUID(), "Pending", Collections.emptyMap(),
                                Collections.singletonMap("k", "v"));
        final var t = new Toleration();
        t.setKey("k1");
        t.setOperator("Equal");
        t.setValue("v1");
        t.setEffect("NoSchedule");
        pod.getSpec().setTolerations(List.of(t));
        final var container = new Container();
        container.setPorts(List.of(port("127.0.0.1/8080/UDP")));
        final ResourceRequirements resourceRequirements = new ResourceRequirements();
        resourceRequirements.setRequests(Collections.emptyMap());
        container.setResources(resourceRequirements);
        container.setName("ignore");
        container.setImage("ignore");
        pod.getSpec().setContainers(List.of(container));
        pod.getSpec().setTopologySpreadConstraints(List.of(spread(1)));
        handler.handle(new PodEvent(PodEvent.Action.ADDED, pod));
        assertTrue(conn.fetch(Tables.POD_INFO).size() > 0);
        assertTrue(conn.fetch(Tables.POD_LABELS).size() > 0);
        assertTrue(conn.fetch(Tables.POD_IMAGES).size() > 0);
        assertTrue(conn.fetch(Tables.POD_TOLERATIONS).size() > 0);
        assertTrue(conn.fetch(Tables.POD_RESOURCE_DEMANDS).size() > 0);
        assertTrue(conn.fetch(Tables.POD_PORTS_REQUEST).size() > 0);
        assertTrue(conn.fetch(Tables.POD_TOPOLOGY_SPREAD_CONSTRAINTS).size() > 0);
        System.out.println(conn.fetch(Tables.POD_INFO));
        final Pod newPod = newPod(podName, UUID.fromString(pod.getMetadata().getUid()), "Running",
                                  Collections.emptyMap(), Collections.singletonMap("k", "v"));
        assertEquals(newPod.getMetadata().getUid(), pod.getMetadata().getUid());
        newPod.getMetadata().setResourceVersion(pod.getMetadata().getResourceVersion() + "1");
        handler.handle(new PodEvent(PodEvent.Action.UPDATED, newPod, pod));
        final var podInfoRecord = conn.selectFrom(Tables.POD_INFO)
                .where(DSL.field(Tables.POD_INFO.UID.getUnqualifiedName()).eq(pod.getMetadata().getUid()))
                .fetchOne();
        System.out.println(conn.fetch(Tables.POD_INFO));
        assertEquals("Running", podInfoRecord.getStatus());

        handler.handle(new PodEvent(PodEvent.Action.DELETED, pod));
        assertFalse(conn.fetch(Tables.POD_INFO).size() > 0);
        assertFalse(conn.fetch(Tables.POD_LABELS).size() > 0);
        assertFalse(conn.fetch(Tables.POD_IMAGES).size() > 0);
        assertFalse(conn.fetch(Tables.POD_TOLERATIONS).size() > 0);
        assertFalse(conn.fetch(Tables.POD_RESOURCE_DEMANDS).size() > 0);
        assertFalse(conn.fetch(Tables.POD_PORTS_REQUEST).size() > 0);
        assertFalse(conn.fetch(Tables.POD_TOPOLOGY_SPREAD_CONSTRAINTS).size() > 0);
        System.out.println(conn.fetch(Tables.POD_INFO));
    }

    /*
     * Test pod whether DCM modifying records does not affect future fetches
     */
    @Test
    public void testPodToDbModificationBug() {
        final DDlogDBConnectionPool pool = setupDDlog();
        final DSLContext conn = pool.getConnectionToDb();
        final PodEventsToDatabase handler = new PodEventsToDatabase(pool);
        final String podName = "p1";
        final Pod pod = newPod(podName, UUID.randomUUID(), "Pending", Collections.emptyMap(),
                Collections.singletonMap("k", "v"));
        handler.handle(new PodEvent(PodEvent.Action.ADDED, pod));
        conn.update(Tables.POD_INFO)
                .set(DSL.field(Tables.POD_INFO.NODE_NAME.getUnqualifiedName()), "node-1")
                .where(DSL.field(Tables.POD_INFO.UID.getUnqualifiedName()).eq(pod.getMetadata().getUid())).execute();
        final var podInfoRecord = conn.selectFrom(Tables.POD_INFO)
                .where(DSL.field(Tables.POD_INFO.UID.getUnqualifiedName()).eq(pod.getMetadata().getUid()))
                .fetchOne();
        assertEquals("node-1", podInfoRecord.getNodeName());
        final Result<Record> records = pool.getProvider().fetchTable("POD_INFO");
        for (final Record record : records) {
            if (record.get(1, String.class).equals("p1")) {
                final Object[] obj = new Object[1];
                obj[0] = "n4";
                record.from(obj, 3);
            }
        }
        final var recordAgain = pool.getProvider().fetchTable("POD_INFO")
                .stream().filter(r -> r.get(1, String.class).equals("p1")).toList();
        assertEquals("node-1", recordAgain.get(0).get(3));

        final var recordAgainViaSelect = conn.selectFrom(Tables.POD_INFO).fetch()
                .stream().filter(r -> r.get(1, String.class).equals("p1")).toList();
        assertEquals("node-1", recordAgainViaSelect.get(0).get(3));
    }

    /*
     * Verify that array types are correctly dumped/reloaded
     */
    @CartesianProductTest
    @CartesianValueSource(booleans = { false, true })
    public void testDebugUtilsForArrays(final boolean scope) {
        final IConnectionPool pool = setupDDlog();
        final var result = TestScenario.withPolicies(Policies.getInitialPlacementPolicies(), scope, pool)
                .withNodeGroup("nodes", 3)
                .withPodGroup("withConstraint", 3, (pod) -> {
                    pod.getMetadata().setLabels(Map.of("k1", "v1"));
                    final PodAffinity podAffinity = new PodAffinity();
                    final List<PodAffinityTerm> podAffinityTerms =
                            podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
                    final String topologyKey = "kubernetes.io/hostname";
                    final List<PodAffinityTerm> inTerm = List.of(term(topologyKey,
                            podExpr("k1", "In", "l1", "l2")));
                    podAffinityTerms.addAll(inTerm);
                    pod.getSpec().getAffinity().setPodAffinity(podAffinity);
                })
                .withPodGroup("remaining", 5)
                .build();
        final Result<Record> resultBefore = result.conn().getConnectionToDb()
                .selectFrom("pod_affinity_match_expressions").fetch();
        DebugUtils.dbDump(result.conn().getConnectionToDb(), UUID.fromString("daed6555-0ea2-419c-8530-403d2825ea8c"));

        final var conn = new DBConnectionPool().getConnectionToDb();
        DebugUtils.dbLoad(conn, "/tmp/debug_daed6555-0ea2-419c-8530-403d2825ea8c");
        final Result<Record> resultAfter = conn.selectFrom("pod_affinity_match_expressions").fetch();
        assertEquals(resultBefore.toString(), resultAfter.toString());
    }


    /*
     * Tests different QoS configurations using two containers and different requests/limits for each
     */
    @ParameterizedTest
    @MethodSource("testQosConditions")
    public void testQoS(final List<Integer> cpuRequests, final List<Integer> cpuLimit,
                        final List<Integer> memoryRequests, final List<Integer> memoryLimit,
                        final PodEventsToDatabase.QosClass expected) {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final PodEventsToDatabase handler = new PodEventsToDatabase(dbConnectionPool);

        final Pod pod = newPod("pod1");
        pod.getSpec().setContainers(new ArrayList<>());

        for (int i = 0; i < 2; i++) {
            final Map<String, Quantity> requests = new HashMap<>();
            if (i <= cpuRequests.size() - 1) {
                requests.put("cpu", new Quantity(String.valueOf(cpuRequests.get(i))));
            }
            if (i <= memoryRequests.size() - 1) {
                requests.put("memory", new Quantity(String.valueOf(memoryRequests.get(i))));
            }
            requests.put("pods", new Quantity("1"));

            final Map<String, Quantity> limits = new HashMap<>();
            if (i <= cpuLimit.size() - 1) {
                limits.put("cpu", new Quantity(String.valueOf(cpuLimit.get(i))));
            }
            if (i <= memoryLimit.size() - 1) {
                limits.put("memory", new Quantity(String.valueOf(memoryLimit.get(i))));
            }
            limits.put("pods", new Quantity("1"));

            final Container container = new Container();
            container.setName("c" + i);
            container.setImage("ignore");

            final ResourceRequirements resourceRequirements = new ResourceRequirements();
            resourceRequirements.setRequests(requests);
            resourceRequirements.setLimits(limits);
            container.setResources(resourceRequirements);

            pod.getSpec().getContainers().add(container);
        }
        handler.handle(new PodEvent(PodEvent.Action.ADDED, pod));

        final List<String> results = conn.selectFrom(Tables.POD_INFO).fetch(Tables.POD_INFO.QOS_CLASS);
        assertEquals(1, results.size());
        assertEquals(expected.toString(), results.get(0));
    }

    public static Stream<Arguments> testQosConditions() {
        return Stream.of(
                Arguments.of(List.of(1, 1), List.of(1, 1), List.of(10, 10), List.of(10, 10),
                             PodEventsToDatabase.QosClass.Guaranteed),
                Arguments.of(List.of(2, 1), List.of(5, 1), List.of(10, 10), List.of(10, 10),
                             PodEventsToDatabase.QosClass.Burstable),
                Arguments.of(List.of(), List.of(), List.of(10, 10), List.of(10, 10),
                             PodEventsToDatabase.QosClass.Burstable),
                Arguments.of(List.of(), List.of(), List.of(), List.of(),
                             PodEventsToDatabase.QosClass.BestEffort)
        );
    }

    /*
     * Since we use strings to encode policy names, double check that we haven't added duplicate names.
     */
    @Test
    public void testNoDuplicatePolicyNames() {
        final List<String> namesList = Policies.getInitialPlacementPolicies();
        final Set<String> namesSet = new HashSet<>(Policies.getInitialPlacementPolicies());
        assertEquals(namesList.size(), namesSet.size());
    }

    /*
     * Evaluates the node predicates policy. One node is free of any node conditions, so all pod
     * assignments must go to that node.
     */
    @CartesianProductTest
    public void testSchedulerNodePredicates(final List<String> condition, final boolean scope) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                                                           Policies.disallowNullNodeSoft());
        final String type = condition.get(0);
        final String status = condition.get(1);
        TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("badNodes", 5, (node) -> {
                    final NodeCondition badCondition = new NodeCondition();
                    badCondition.setStatus(status);
                    badCondition.setType(type);
                    node.getStatus().setConditions(List.of(badCondition));
                })
                .withNodeGroup("goodNodes", 1)
                .withPodGroup("p", 10)
                .runInitialPlacement()
                .expect(nodesForPodGroup("p"), IN, nodeGroup("goodNodes"))
                .expect(nodesForPodGroup("p"), NOT_IN, nodeGroup("badNodes"));
    }

    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testSchedulerNodePredicates() {
        return new CartesianProductTest.Sets()
                .add(Arrays.asList("OutOfDisk", "True"),
                    Arrays.asList("MemoryPressure", "True"),
                    Arrays.asList("DiskPressure", "True"),
                    Arrays.asList("PIDPressure", "True"),
                    Arrays.asList("NetworkUnavailable", "True"),
                    Arrays.asList("Ready", "False"))
                .add(true, false);
    }

    /*
     * Tests the pod_node_selector_matches view.
     */
    @CartesianProductTest
    @CartesianValueSource(booleans = { false, true })
    public void testPodNodeSelector(final boolean scope) {
            final IConnectionPool dbConnectionPool = setupDDlog();
        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                                                           Policies.disallowNullNodeSoft(),
                                                                           Policies.nodeSelectorPredicate());
        TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("noLabels", 5)
                .withNodeGroup("fullMatch", 5,
                               (node) -> node.getMetadata().setLabels(Map.of("diskType", "ssd", "gpu", "true")))
                .withNodeGroup("partialMatch", 5,
                               (node) -> node.getMetadata().setLabels(Map.of("diskType", "ssd")))
                .withPodGroup("noLabels", 5)
                .withPodGroup("fullMatch", 5,
                               (pod) -> pod.getSpec().setNodeSelector(Map.of("diskType", "ssd", "gpu", "true")))
                .withPodGroup("partialMatch", 5,
                               (pod) -> pod.getSpec().setNodeSelector(Map.of("diskType", "ssd")))
                .runInitialPlacement()
                .expect(nodesForPodGroup("fullMatch"), IN, nodeGroup("fullMatch"))
                .expect(nodesForPodGroup("fullMatch"), NOT_IN, nodeGroup("partialMatch", "noLabels"))
                .expect(nodesForPodGroup("partialMatch"), IN, nodeGroup("fullMatch", "partialMatch"))
                .expect(nodesForPodGroup("partialMatch"), NOT_IN, nodeGroup("noLabels"))
                .expect(nodesForPodGroup("noLabels"), IN, nodeGroup("fullMatch", "partialMatch", "noLabels"));
    }

    /*
     * Tests the pod_node_selector_matches view.
     */
    @CartesianProductTest
    public void testPodToNodeAffinity(final List<Object> args, final boolean scope) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        // Unpack arguments
        final List<NodeSelectorTerm> terms = (List<NodeSelectorTerm>) args.get(0);
        final Map<String, String> nodeLabelsInput = (Map<String, String>) args.get(1);
        final boolean shouldBeAffineToLabelledNodes = (boolean) args.get(2);
        final boolean shouldBeAffineToRemainingNodes = (boolean) args.get(3);
        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                                                           Policies.disallowNullNodeSoft(),
                                                                           Policies.nodeSelectorPredicate());
        final Map<String, String> dummyLabels = Map.of("dummyKey1", "dummyValue1", "dummyKey2", "dummyValue2");
        final var result = TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("providedLabels", 5, (node) -> node.getMetadata().setLabels(nodeLabelsInput))
                .withNodeGroup("dummyLabels", 5, (node) -> node.getMetadata().setLabels(dummyLabels))
                .withPodGroup("withConstraint", 3, (pod) -> {
                    final NodeSelector selector = new NodeSelectorBuilder().withNodeSelectorTerms(terms).build();
                    pod.getSpec().getAffinity().getNodeAffinity()
                            .setRequiredDuringSchedulingIgnoredDuringExecution(selector);
                })
                .withPodGroup("remaining", 17)
                .runInitialPlacement()
                // Pods without constraints should always be placed and can go anywhere
                .expect(nodesForPodGroup("remaining"), IN, nodeGroup("providedLabels", "dummyLabels"));

        if (shouldBeAffineToLabelledNodes && shouldBeAffineToRemainingNodes) {
            result.expect(nodesForPodGroup("withConstraint"), IN, nodeGroup("providedLabels", "dummyLabels"));
        } else if (shouldBeAffineToLabelledNodes) {
            result.expect(nodesForPodGroup("withConstraint"), IN, nodeGroup("providedLabels"));
        } else if (shouldBeAffineToRemainingNodes) {
            result.expect(nodesForPodGroup("withConstraint"), IN, nodeGroup("dummyLabels"));
        } else {
            result.expect(nodesForPodGroup("withConstraint"), EQUALS, nodeGroup("NULL_NODE"));
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testPodToNodeAffinity() {
        final List<NodeSelectorTerm> inTerm = List.of(term(nodeExpr("k1", "In", "l1", "l2")));
        final List<NodeSelectorTerm> existsTerm = List.of(term(nodeExpr("k1", "Exists", "l1", "l2")));
        /*
        final List<NodeSelectorTerm> notInTerm = List.of(term(nodeExpr("k1", "NotIn", "l1", "l2")));
        final List<NodeSelectorTerm> notExistsTerm = List.of(term(nodeExpr("k1", "DoesNotExist", "l1", "l2")));
        */
        return new CartesianProductTest.Sets()
                .add(// First, we test to see if all our operators work on their own
                    // In
                    Arrays.asList(inTerm, map("k1", "l1"), true, false),
                    Arrays.asList(inTerm, map("k1", "l2"), true, false),
                    Arrays.asList(inTerm, map("k1", "l3"), false, false),
                    Arrays.asList(inTerm, map("k", "l", "k1", "l1"), true, false),
                    Arrays.asList(inTerm, map("k", "l", "k1", "l2"), true, false),
                    Arrays.asList(inTerm, map("k", "l", "k1", "l3"), false, false),

                    // Exists
                    Arrays.asList(existsTerm, map("k1", "l1"), true, false),
                    Arrays.asList(existsTerm, map("k1", "l2"), true, false),
                    Arrays.asList(existsTerm, map("k1", "l3"), true, false),
                    Arrays.asList(existsTerm, map("k2", "l3"), false, false),
                    Arrays.asList(existsTerm, map("k2", "l1"), false, false),
                    Arrays.asList(existsTerm, map("k", "l", "k1", "l1"), true, false),
                    Arrays.asList(existsTerm, map("k", "l", "k1", "l2"), true, false),
                    Arrays.asList(existsTerm, map("k", "l", "k1", "l3"), true, false),
                    Arrays.asList(existsTerm, map("k", "l", "k2", "l1"), false, false),
                    Arrays.asList(existsTerm, map("k", "l", "k2", "l2"), false, false),
                    Arrays.asList(existsTerm, map("k", "l", "k2", "l3"), false, false)

                    // NotIn
                    /* TODO: temporarily disabled until non-aggregate columns in aggregate functions are
                             are correctly handled
                    Arrays.asList(notInTerm, map("k1", "l1"), false, true),
                    Arrays.asList(notInTerm, map("k1", "l2"), false, true),
                    Arrays.asList(notInTerm, map("k1", "l3"), true, true),
                    Arrays.asList(notInTerm, map("k", "l", "k1", "l1"), false, true),
                    Arrays.asList(notInTerm, map("k", "l", "k1", "l2"), false, true),
                    Arrays.asList(notInTerm, map("k", "l", "k1", "l3"), true, true),

                    // DoesNotExist
                    Arrays.asList(notExistsTerm, map("k1", "l1"), false, true),
                    Arrays.asList(notExistsTerm, map("k1", "l2"), false, true),
                    Arrays.asList(notExistsTerm, map("k1", "l3"), false, true),
                    Arrays.asList(notExistsTerm, map("k", "l", "k1", "l1"), false, true),
                    Arrays.asList(notExistsTerm, map("k", "l", "k1", "l2"), false, true),
                    Arrays.asList(notExistsTerm, map("k", "l", "k1", "l3"), false, true))

                     */
                    )
                .add(false, true);
    }


    /*
     * Tests inter-pod affinity behavior
     */
    @CartesianProductTest(name = "Scope={0}, Scenario={1}")
    public void testPodToPodAffinity(final boolean scope, final TestArguments args) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        // Unpack arguments
        final List<PodAffinityTerm> terms = (List<PodAffinityTerm>) args.get(1);
        final Map<String, String> podLabelsInput = (Map<String, String>) args.get(2);
        final boolean affineToLabelledPods = (boolean) args.get(3);
        final boolean affineToRemainingPods = (boolean) args.get(4);
        final boolean cannotBePlacedAnywhere = (boolean) args.get(5);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                                                           Policies.disallowNullNodeSoft(),
                                                                           Policies.podAffinityPredicate(),
                                                                           Policies.podAntiAffinityPredicate());
        final var result = TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("nodes", 3)
                .withPodGroup("withConstraint", 3, (pod) -> {
                    pod.getMetadata().setLabels(podLabelsInput);
                    final PodAffinity podAffinity = new PodAffinity();
                    final List<PodAffinityTerm> podAffinityTerms =
                            podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
                    podAffinityTerms.addAll(terms);
                    pod.getSpec().getAffinity().setPodAffinity(podAffinity);
                })
                .withPodGroup("remaining", 5,
                              (pod) -> pod.getMetadata().setLabels(Collections.singletonMap("dummyKey", "dummyValue")))
                .runInitialPlacement();
        if (cannotBePlacedAnywhere) {
            result.expect(nodesForPodGroup("withConstraint"), EQUALS, nodeGroup("NULL_NODE"));
            return;
        }
        result.expect(nodesForPodGroup("withConstraint"), NOT_EQUALS, nodeGroup("NULL_NODE"));
        if (affineToLabelledPods && affineToRemainingPods) {
            result.expect(podGroup("withConstraint"), COLOCATED_WITH, podGroup("withConstraint", "remaining"));
        } else if (affineToLabelledPods) {
            result.expect(podGroup("withConstraint"), COLOCATED_WITH, podGroup("withConstraint"));
        } else if (affineToRemainingPods) {
            result.expect(podGroup("withConstraint"), COLOCATED_WITH, podGroup("remaining"));
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testPodToPodAffinity() {
        final String topologyKey = "kubernetes.io/hostname";
        final List<PodAffinityTerm> inTerm = List.of(term(topologyKey,
                                                       podExpr("k1", "In", "l1", "l2")));
        final List<PodAffinityTerm> existsTerm = List.of(term(topologyKey,
                                                                podExpr("k1", "Exists", "l1", "l2")));
        /*
        final List<PodAffinityTerm> notInTerm = List.of(term(topologyKey,
                                                          podExpr("k1", "NotIn", "l1", "l2")));
        final List<PodAffinityTerm> notExistsTerm = List.of(term(topologyKey,
                                                                   podExpr("k1", "DoesNotExist", "l1", "l2")));
         */
        return new CartesianProductTest.Sets()
            .add(false, true)
            .add(
                // --------- Pod Affinity -----------
                // In
                argGen("Affinity", inTerm, map("k1", "l1"), true, false, false),
                argGen("Affinity", inTerm, map("k1", "l2"), true, false, false),
                argGen("Affinity", inTerm, map("k1", "l3"), false, false, true),
                argGen("Affinity", inTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("Affinity", inTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("Affinity", inTerm, map("k", "l", "k1", "l3"), false, false, true),

                // Exists
                argGen("Affinity", existsTerm, map("k1", "l1"), true, false, false),
                argGen("Affinity", existsTerm, map("k1", "l2"), true, false, false),
                argGen("Affinity", existsTerm, map("k1", "l3"), true, false, false),
                argGen("Affinity", existsTerm, map("k2", "l3"), false, false, true),
                argGen("Affinity", existsTerm, map("k2", "l1"), false, false, true),
                argGen("Affinity", existsTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("Affinity", existsTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("Affinity", existsTerm, map("k", "l", "k1", "l3"), true, false, false),
                argGen("Affinity", existsTerm, map("k", "l", "k2", "l1"), false, false, true),
                argGen("Affinity", existsTerm, map("k", "l", "k2", "l2"), false, false, true),
                argGen("Affinity", existsTerm, map("k", "l", "k2", "l3"), false, false, true));

                // NotIn
                /* TODO: temporarily disabled until non-aggregate columns in aggregate functions are
                         are correctly handled
                argGen("Affinity", notInTerm, map("k1", "l1"), false, true, false),
                argGen("Affinity", notInTerm, map("k1", "l2"), false, true, false),
                argGen("Affinity", notInTerm, map("k1", "l3"), true, true, false),
                argGen("Affinity", notInTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("Affinity", notInTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("Affinity", notInTerm, map("k", "l", "k1", "l3"), true, true, false),

                // DoesNotExist
                argGen("Affinity", notExistsTerm, map("k1", "l1"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k1", "l2"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k1", "l3"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("Affinity", notExistsTerm, map("k", "l", "k1", "l3"), false, true, false));
                 */
    }


    /*
     * Tests inter-pod anti-affinity behavior
     */
    @CartesianProductTest(name = "{0}")
    public void testPodToPodAntiAffinity(final TestArguments args, final boolean scope) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        // Unpack arguments
        final List<PodAffinityTerm> terms = (List<PodAffinityTerm>) args.get(1);
        final Map<String, String> podLabelsInput = (Map<String, String>) args.get(2);
        final boolean antiAffineToLabelledPods = (boolean) args.get(3);
        final boolean antiAffineToRemainingPods = (boolean) args.get(4);
        final boolean cannotBePlacedAnywhere = (boolean) args.get(5);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                Policies.disallowNullNodeSoft(),
                Policies.podAntiAffinityPredicate());
        final var result = TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("nodes", 3)
                .withPodGroup("withConstraint", 3, (pod) -> {
                    pod.getMetadata().setLabels(podLabelsInput);
                    final PodAntiAffinity podAntiAffinity = new PodAntiAffinity();
                    final List<PodAffinityTerm> podAntiAffinityTerms =
                            podAntiAffinity.getRequiredDuringSchedulingIgnoredDuringExecution();
                    podAntiAffinityTerms.addAll(terms);
                    pod.getSpec().getAffinity().setPodAntiAffinity(podAntiAffinity);
                })
                .withPodGroup("remaining", 5,
                              (pod) -> pod.getMetadata().setLabels(Collections.singletonMap("dummyKey", "dummyValue")))
                .runInitialPlacement();
        if (cannotBePlacedAnywhere) {
            result.expect(nodesForPodGroup("withConstraint"), EQUALS, nodeGroup("NULL_NODE"));
            return;
        }
        result.expect(nodesForPodGroup("withConstraint"), NOT_EQUALS, nodeGroup("NULL_NODE"));
        if (antiAffineToLabelledPods && antiAffineToRemainingPods) {
            // In this configuration, there should be exactly one NULL_NODE assignment (because all pods
            // from the 'remaining' group go to one node, and then there's 2 nodes left for 3 pods that
            // are anti-affine to each other).
            result.expect(podGroup("withConstraint"), NOT_COLOCATED_WITH, podGroup("withConstraint"));
            result.expect(nodesForPodGroup("withConstraint"),
                          nodes -> nodes.stream().filter(e -> e.equals("NULL_NODE")).count() == 1);
        } else if (antiAffineToLabelledPods) {
            result.expect(podGroup("withConstraint"), NOT_COLOCATED_WITH, podGroup("withConstraint"));
        } else if (antiAffineToRemainingPods) {
            result.expect(podGroup("withConstraint"), NOT_COLOCATED_WITH, podGroup("remaining"));
        } else { // can be placed anywhere
            result.expect(podGroup("withConstraint"), COLOCATED_WITH, podGroup("withConstraint", "remaining"));
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testPodToPodAntiAffinity() {
        final String topologyKey = "kubernetes.io/hostname";
        final List<PodAffinityTerm> inTerm = List.of(term(topologyKey,
                podExpr("k1", "In", "l1", "l2")));
        final List<PodAffinityTerm> existsTerm = List.of(term(topologyKey,
                podExpr("k1", "Exists", "l1", "l2")));
        /*
        final List<PodAffinityTerm> notInTerm = List.of(term(topologyKey,
                podExpr("k1", "NotIn", "l1", "l2")));
        final List<PodAffinityTerm> notExistsTerm = List.of(term(topologyKey,
                podExpr("k1", "DoesNotExist", "l1", "l2")));
        */
        return new CartesianProductTest.Sets()
            .add(
                // --------- Pod Anti Affinity -----------
                // In
                argGen("AntiAffinity", inTerm, map("k1", "l1"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k1", "l2"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k1", "l3"), false, false, false),
                argGen("AntiAffinity", inTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("AntiAffinity", inTerm, map("k", "l", "k1", "l3"), false, false, false),

                // Exists
                argGen("AntiAffinity", existsTerm, map("k1", "l1"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k1", "l2"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k1", "l3"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k2", "l3"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k2", "l1"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k1", "l1"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k1", "l2"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k1", "l3"), true, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k2", "l1"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k2", "l2"), false, false, false),
                argGen("AntiAffinity", existsTerm, map("k", "l", "k2", "l3"), false, false, false)

                // NotIn
                /* TODO: temporarily disabled until non-aggregate columns in aggregate functions are
                         are correctly handled
                argGen("AntiAffinity", notInTerm, map("k1", "l1"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k1", "l2"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k1", "l3"), true, true, false),
                argGen("AntiAffinity", notInTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("AntiAffinity", notInTerm, map("k", "l", "k1", "l3"), true, true, false),
                argGen("AntiAffinity", notInTerm, map("k", "l", "k1", "l2"), false, true, false),

                // DoesNotExist
                argGen("AntiAffinity", notExistsTerm, map("k1", "l1"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k1", "l2"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k1", "l3"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k", "l", "k1", "l1"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k", "l", "k1", "l2"), false, true, false),
                argGen("AntiAffinity", notExistsTerm, map("k", "l", "k1", "l3"), false, true, false))
                 */
            )
            .add(false, true);
    }

    private static TestArguments argGen(final String scenario, final List<PodAffinityTerm> terms,
                                    final Map<String, String> podLabelsInput,
                                    final boolean shouldBeAffineToLabelledPods,
                                    final boolean shouldBeAffineToRemainingPods,
                                    final boolean cannotBePlacedAnywhere) {
        final String termsString = terms.stream().map(PodAffinityTerm::getLabelSelector)
                .map(expr -> expr.getMatchExpressions().stream()
                        .map(e -> String.format("%s %s %s", e.getKey(), e.getOperator(), e.getValues()))
                        .collect(Collectors.joining(", "))
                ).collect(Collectors.joining(" or "));
        final String effect = scenario.equals("AntiAffinity") ? "anti-affine" : "affine";
        final String outcomeString =  List.of(
                shouldBeAffineToLabelledPods ? String.format("should be %s to themselves", effect) : "",
                shouldBeAffineToRemainingPods ? String.format("should be %s to remaining pods", effect) : "",
                cannotBePlacedAnywhere ? "cannot be placed anywhere" : "",
                !shouldBeAffineToLabelledPods && !shouldBeAffineToRemainingPods && !cannotBePlacedAnywhere
                     ? "can be placed anywhere" : "")
                .stream().filter(e -> e.length() > 0)
                .collect(Collectors.joining(" and "));
        final String label = String.format("%s: pods with term {%s} and labels %s, %s", scenario, termsString,
                podLabelsInput, outcomeString);
        return new TestArguments(Arrays.asList(label, terms, podLabelsInput, shouldBeAffineToLabelledPods,
                            shouldBeAffineToRemainingPods, cannotBePlacedAnywhere));
    }

    /*
     * Capacity constraints
     */
    @CartesianProductTest(name = "Scope={1}, Scenario={0}")
    public void testSpareCapacity(final TestArguments args, final boolean scope) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        // Unpack arguments
        final List<Integer> cpuRequests = (List<Integer>) args.get(1);
        final List<Integer> memoryRequests = (List<Integer>) args.get(2);
        final List<Integer> nodeCpuCapacities = (List<Integer>) args.get(3);
        final List<Integer> nodeMemoryCapacities = (List<Integer>) args.get(4);
        final boolean useHardConstraint = (boolean) args.get(5);
        final boolean useSoftConstraint = (boolean) args.get(6);
        final Predicate<List<String>> assertOn = (Predicate<List<String>>) args.get(7);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                            Policies.disallowNullNodeSoft(),
                                            Policies.capacityConstraint(useHardConstraint, useSoftConstraint));
        final Iterator<Integer> cpuCapIt = nodeCpuCapacities.iterator();
        final Iterator<Integer> memCapIt = nodeMemoryCapacities.iterator();
        final Iterator<Integer> cpuReqIt = cpuRequests.iterator();
        final Iterator<Integer> memReqIt = memoryRequests.iterator();
        final var scenario = TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("n", nodeCpuCapacities.size(), (node) -> {
                    node.getStatus().getCapacity().put("cpu",
                            new Quantity(String.valueOf(cpuCapIt.next())));
                    node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(memCapIt.next())));
                })
                .withPodGroup("pods", cpuRequests.size(), pod -> {
                    final Map<String, Quantity> resourceRequests = new HashMap<>();
                    resourceRequests.put("cpu", new Quantity(String.valueOf(cpuReqIt.next())));
                    resourceRequests.put("memory", new Quantity(String.valueOf(memReqIt.next())));
                    resourceRequests.put("pods", new Quantity("1"));
                    // Assumes that there is only one container
                    pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
                });
        scenario.runInitialPlacement().expect(nodesForPodGroup("pods"), assertOn);
    }


    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testSpareCapacity() {
        final Predicate<List<String>> onePodPerNode = nodes -> nodes.size() == Set.copyOf(nodes).size();
        final Predicate<List<String>> n2MustNotBeAssignedNewPods = nodes -> !nodes.contains("n-2");
        final Predicate<List<String>> onlyN3MustBeAssignedNewPods = nodes -> Set.of("n-3").equals(Set.copyOf(nodes));
        final Predicate<List<String>> p1CannotBePlaced = nodes -> nodes.stream()
                                                                 .filter(e -> e.equals("NULL_NODE")).count() == 1;
        return new CartesianProductTest.Sets()
            .add(
                new TestArguments(Arrays.asList("One pod per node",
                             List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                             List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10), true, false,
                             onePodPerNode)),

                new TestArguments(Arrays.asList("p1 cannot be placed",
                        List.of(10, 11, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        List.of(10, 10, 10, 10, 10), List.of(10, 10, 10, 10, 10), true, false,
                        p1CannotBePlaced)),

                new TestArguments(Arrays.asList(
                        "n2 does not have sufficient CPU capacity and should not host any new pods",
                        List.of(5, 5, 10, 10, 10), List.of(10, 10, 10, 10, 10),
                        List.of(10, 10, 1, 10, 10), List.of(20, 20, 20, 20, 20), true, false,
                        n2MustNotBeAssignedNewPods)),

                new TestArguments(Arrays.asList("Only memory requests, and all pods must go to n3",
                        List.of(0, 0, 0, 0, 0), List.of(10, 10, 10, 10, 10),
                        List.of(1, 1, 1, 1, 1), List.of(1, 1, 1, 50, 1), true, false,
                        onlyN3MustBeAssignedNewPods)),

                new TestArguments(Arrays.asList("Only memory requests, only soft constraints, pods must be spread out",
                        List.of(0, 0, 0), List.of(10, 10, 10),
                        List.of(1, 1, 1, 1, 1), List.of(100, 100, 100, 100, 100), false, true,
                        onePodPerNode)))
            .add(false, true);
    }

    /*
     * Test capacity constraints on custom resources along with overheads.
     * TODO: Disabled until cross apply is correctly handled
     */
    @CartesianProductTest(name = "{0}")
    @Disabled
    public void testCustomResources(final TestArguments args, final boolean scope) {
        // Unpack arguments
        final int capacity = (int) args.get(1);
        final int demand = (int) args.get(2);
        final int overhead = (int) args.get(3);
        final boolean feasible = (boolean) args.get(4);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                Policies.disallowNullNodeSoft(),
                Policies.capacityConstraint(true, true));
        final var scenario = TestScenario.withPolicies(policies, scope)
                .withNodeGroup("n", 1,
                               (node) -> {
                                    if (capacity >= 0) {
                                        node.getStatus().getCapacity().put("some-resource",
                                                new Quantity(String.valueOf(capacity)));
                                    }
                               })
                .withPodGroup("pods", 1,
                               pod -> {
                                   final Map<String, Quantity> resourceRequests = new HashMap<>();
                                   if (demand >= 0) {
                                       resourceRequests.put("some-resource", new Quantity(String.valueOf(demand)));
                                   }
                                   // Assumes that there is only one container
                                   pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
                                   final Map<String, Quantity> overheads = new HashMap<>();
                                   if (overhead >= 0) {
                                       overheads.put("some-resource", new Quantity(String.valueOf(overhead)));
                                       pod.getSpec().setOverhead(overheads);
                                   }
                               });
        if (feasible) {
            scenario.runInitialPlacement().expect(nodesForPodGroup("pods"), EQUALS, nodeGroup("n"));
        } else {
            scenario.runInitialPlacement().expect(nodesForPodGroup("pods"), EQUALS, nodeGroup("NULL_NODE"));
        }
    }

    @SuppressWarnings("unused")
    private static CartesianProductTest.Sets testCustomResources() {
        return new CartesianProductTest.Sets()
            .add(
                new TestArguments(
                        Arrays.asList("Resource does not exist on host", -1, 5, -1, false)),
                new TestArguments(
                        Arrays.asList("Resource exists, but no capacity", 0, 5, -1, false)),
                new TestArguments(
                        Arrays.asList("Resource exists, has enough capacity for demand ", 5, 5, -1, true)),
                new TestArguments(
                        Arrays.asList("Resource exists, has enough capacity for demand + overhead ", 5, 3, 2, true)),
                new TestArguments(
                        Arrays.asList("Resource exists, but not enough capacity for demand", 5, 6, -1, false)),
                new TestArguments(
                    Arrays.asList("Resource exists, has enough capacity for demand, but not overhead", 5, 3, 3, false)),
                new TestArguments(
                    Arrays.asList("Resource exists, but is not demanded", 5, -1, -1, true)))
            .add(false, true);
    }

    @CartesianProductTest(name = "{0}")
    public void testTaintsAndTolerations(final TestArguments args, final boolean scope) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        // Unpack arguments
        final List<List<Toleration>> tolerations = (List<List<Toleration>>) args.get(1);
        final List<List<Taint>> taints = (List<List<Taint>>) args.get(2);
        final Predicate<List<String>> assertOn = (Predicate<List<String>>) args.get(3);
        final boolean feasible = (boolean) args.get(4);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                                                   Policies.disallowNullNodeSoft(),
                                                                   Policies.taintsAndTolerations());
        final Iterator<List<Toleration>> tolerationsIt = tolerations.iterator();
        final Iterator<List<Taint>> taintsIt = taints.iterator();
        final var result = TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("n", taints.size(), node -> node.getSpec().setTaints(taintsIt.next()))
                .withPodGroup("pods", tolerations.size(), pod -> {
                    final List<Toleration> t = tolerationsIt.next();
                    if (t.size() != 0) {
                        pod.getSpec().setTolerations(t);
                    }
                })
                .runInitialPlacement();
        if (feasible) {
            result.expect(nodesForPodGroup("pods"), assertOn);
        } else {
            result.expect(nodesForPodGroup("pods"), EQUALS, nodeGroup("NULL_NODE"));
        }
    }


    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testTaintsAndTolerations() {
        final Toleration tolerateK1Equals = new Toleration();
        tolerateK1Equals.setKey("k1");
        tolerateK1Equals.setOperator("Equal");
        tolerateK1Equals.setValue("v1");
        tolerateK1Equals.setEffect("NoSchedule");

        final Toleration tolerateK2Exists = new Toleration();
        tolerateK2Exists.setKey("k2");
        tolerateK2Exists.setOperator("Exists");
        tolerateK2Exists.setEffect("NoSchedule");

        final Toleration tolerateK1Exists = new Toleration();
        tolerateK1Exists.setKey("k1");
        tolerateK1Exists.setOperator("Exists");
        tolerateK1Exists.setEffect("NoSchedule");

        final Toleration tolerateK1ExistsNoExecute = new Toleration();
        tolerateK1ExistsNoExecute.setKey("k1");
        tolerateK1ExistsNoExecute.setOperator("Exists");
        tolerateK1ExistsNoExecute.setEffect("NoExecute");

        final Taint taintK1 = new Taint();
        taintK1.setKey("k1");
        taintK1.setValue("v1");
        taintK1.setEffect("NoSchedule");

        final Taint taintK1NullValue = new Taint();
        taintK1NullValue.setKey("k1");
        taintK1NullValue.setEffect("NoSchedule");

        final Taint taintK1NoExecute = new Taint();
        taintK1NoExecute.setKey("k1");
        taintK1NoExecute.setValue("v1");
        taintK1NoExecute.setEffect("NoExecute");

        final Taint taintK1V2 = new Taint();
        taintK1V2.setKey("k1");
        taintK1V2.setValue("v2");
        taintK1V2.setEffect("NoSchedule");

        final Taint taintK2V4 = new Taint();
        taintK2V4.setKey("k2");
        taintK2V4.setValue("v4");
        taintK2V4.setEffect("NoSchedule");

        final Predicate<List<String>> P0GoesToN0 = nodes -> nodes.size() == 1 && nodes.get(0).equals("n-0");
        final Predicate<List<String>> p0goesToN1andP1GoesToN0 =
                nodes -> nodes.size() == 2 && nodes.get(0).equals("n-1") && nodes.get(1).equals("n-0");
        final Predicate<List<String>> p0goesToN1andP1GoesToN1 =
                nodes -> nodes.size() == 2 && nodes.get(0).equals("n-1") && nodes.get(1).equals("n-1");
        return new CartesianProductTest.Sets()
            .add(
                new TestArguments(
                    Arrays.asList("No toleration => cannot match taint k1:v1",
                        List.of(List.of()), List.of(List.of(taintK1)),
                        null, false)),

                new TestArguments(
                    Arrays.asList("toleration k1=v1 matches taint k1:v1",
                         List.of(List.of(tolerateK1Equals)), List.of(List.of(taintK1)),
                         P0GoesToN0, true)),

                new TestArguments(
                    Arrays.asList("toleration exists(k1) matches taint k1:v1",
                        List.of(List.of(tolerateK2Exists)), List.of(List.of(taintK2V4)),
                        P0GoesToN0, true)),

                new TestArguments(
                    Arrays.asList("toleration k1=v1 does not match taint k1:v2",
                        List.of(List.of(tolerateK1Equals)), List.of(List.of(taintK1V2)),
                        null, false)),

                new TestArguments(
                    Arrays.asList("toleration exists(k1) does not match taint k1:v1",
                        List.of(List.of(tolerateK2Exists)), List.of(List.of(taintK1)),
                        null, false)),

                new TestArguments(
                    Arrays.asList("toleration k1=v1 does not match taints [k1:v1, k2:v4]",
                        List.of(List.of(tolerateK1Equals)), List.of(List.of(taintK1, taintK2V4)),
                        null, false)),

                new TestArguments(
                    Arrays.asList("toleration [k1=v1, exists(k2)] matches taints [k1:v1, k2:v4]",
                        List.of(List.of(tolerateK1Equals, tolerateK2Exists)), List.of(List.of(taintK1, taintK2V4)),
                        P0GoesToN0, true)),

                new TestArguments(
                    Arrays.asList("toleration [k1=v1, exists(k2)] does not match taints [k1:v1, k1:v2, k2:v4]",
                        List.of(List.of(tolerateK1Equals, tolerateK2Exists)),
                        List.of(List.of(taintK1, taintK1V2, taintK2V4)),
                        null, false)),

                new TestArguments(
                    Arrays.asList("toleration [exists(k1), exists(k2)] matches taints [k1:v1, k1:v2, k2:v4]",
                        List.of(List.of(tolerateK1Exists, tolerateK2Exists)),
                        List.of(List.of(taintK1, taintK1V2, taintK2V4)),
                        P0GoesToN0, true)),

                new TestArguments(
                    Arrays.asList(
                        "toleration [exists(k1), exists_noexec(k2)] does not match taints [k1:v1, k1:v2, k2:v4]",
                        List.of(List.of(tolerateK1Exists, tolerateK1ExistsNoExecute)),
                        List.of(List.of(taintK1, taintK1V2, taintK2V4)),
                        null, false)),

                new TestArguments(
                    Arrays.asList("toleration [exists(k1), exists_noexec(k2)] matches taints [k1:v1]",
                        List.of(List.of(tolerateK1Exists, tolerateK1ExistsNoExecute)), List.of(List.of(taintK1)),
                        P0GoesToN0, true)),

                new TestArguments(
                    Arrays.asList("Multi node: p0 should go to n1 and p1 to n0.",
                        List.of(List.of(tolerateK1Equals, tolerateK2Exists),   // pod-0
                                List.of(tolerateK1ExistsNoExecute)),             // pod-1
                        List.of(List.of(taintK1NoExecute),                     // node-0
                                List.of(taintK1)),                             // node-1
                        p0goesToN1andP1GoesToN0, true)),

                new TestArguments(Arrays.asList("Multi node: p0 and p1 cannot tolerate n0 so they go to n1",
                        List.of(List.of(),                                       // pod-0
                                List.of()),                                      // pod-1
                        List.of(List.of(taintK1NullValue, taintK1),            // node-0
                                List.of()),                                      // node-1
                        p0goesToN1andP1GoesToN1, true)))
            .add(false, true);
    }

    @CartesianProductTest(name = "{0}")
    public void testHostPorts(final TestArguments args, final boolean scope) {
        final IConnectionPool dbConnectionPool = setupDDlog();
        // Unpack arguments
        final List<List<ContainerPort>> runningPods = (List<List<ContainerPort>>) args.get(1);
        final List<List<ContainerPort>> podContainers = (List<List<ContainerPort>>) args.get(2);
        final boolean feasible = (boolean) args.get(3);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                Policies.disallowNullNodeSoft(),
                Policies.podFitsNodePorts());
        final Iterator<List<ContainerPort>> nodeContainersIt = runningPods.iterator();
        final Iterator<List<ContainerPort>> podContainersIt = podContainers.iterator();
        final var result = TestScenario.withPolicies(policies, scope, dbConnectionPool)
                .withNodeGroup("n", 1)
                .withPodGroup("runningPods", runningPods.size(), (pod) -> {
                    pod.getSpec().setNodeName("n-0");
                    final List<ContainerPort> ports = nodeContainersIt.next();
                    final var container = new Container();
                    container.setPorts(ports);
                    final ResourceRequirements resourceRequirements = new ResourceRequirements();
                    resourceRequirements.setRequests(Collections.emptyMap());
                    container.setResources(resourceRequirements);
                    container.setName("ignore");
                    container.setImage("ignore");
                    pod.getSpec().setContainers(List.of(container));
                })
                .withPodGroup("newPod", podContainers.size(), pod -> {
                    final List<ContainerPort> ports = podContainersIt.next();
                    final var container = new Container();
                    container.setPorts(ports);
                    final ResourceRequirements resourceRequirements = new ResourceRequirements();
                    resourceRequirements.setRequests(Collections.emptyMap());
                    container.setResources(resourceRequirements);
                    container.setName("ignore");
                    container.setImage("ignore");
                    pod.getSpec().setContainers(List.of(container));
                }).runInitialPlacement();
        if (feasible) {
            result.expect(nodesForPodGroup("newPod"), EQUALS, nodeGroup("n"));
        } else {
            result.expect(nodesForPodGroup("newPod"), EQUALS, nodeGroup("NULL_NODE"));
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testHostPorts() {
        return new CartesianProductTest.Sets()
            .add(
                new TestArguments(Arrays.asList("Conflicting IP, port and TCP protocol",
                            List.of(List.of(port("127.0.0.1/8080/UDP"))),
                            List.of(List.of(port("127.0.0.1/8080/UDP"))),
                            false)),
                new TestArguments(Arrays.asList("Conflicting IP, port and TCP protocol",
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            false)),
                new TestArguments(Arrays.asList("Same IP, port but different protocol",
                            List.of(List.of(port("127.0.0.1/8080/UDP"))),
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            true)),
                new TestArguments(Arrays.asList("Different IP, but port and protocol",
                            List.of(List.of(port("127.0.0.2/8080/UDP"))),
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            true)),
                new TestArguments(Arrays.asList("Same IP, different port, same protocol",
                            List.of(List.of(port("127.0.0.1/8082/UDP"))),
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            true)),
                new TestArguments(Arrays.asList("0.0.0.0 IP, same port, different protocol",
                            List.of(List.of(port("0.0.0.0/8080/UDP"))),
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            true)),
                new TestArguments(Arrays.asList("0.0.0.0 IP, same port, same protocol",
                            List.of(List.of(port("0.0.0.0/8080/TCP"))),
                            List.of(List.of(port("127.0.0.1/8080/TCP"))),
                            false)),
                new TestArguments(Arrays.asList("0.0.0.0 IP, same port, same protocol",
                            List.of(List.of(port("14.0.10.30/8081/UDP"))),
                            List.of(List.of(port("0.0.0.0/8081/UDP"))),
                            false)),
                new TestArguments(Arrays.asList("0.0.0.0 IP, different port, same protocol",
                            List.of(List.of(port("14.0.10.30/8081/UDP"))),
                            List.of(List.of(port("0.0.0.0/8083/UDP"))),
                            true)),
                new TestArguments(Arrays.asList("Multiple pending containers, first one conflicts",
                            List.of(List.of(port("14.0.10.30/8081/UDP"), port("14.0.10.30/8082/UDP"))),
                            List.of(List.of(port("0.0.0.0/8083/UDP"), port("0.0.0.0/8081/UDP"))),
                            false)),
                new TestArguments(Arrays.asList("Multiple pending containers, second one conflicts",
                            List.of(List.of(port("14.0.10.30/8081/UDP"), port("14.0.10.30/8082/UDP"))),
                            List.of(List.of(port("0.0.0.0/8082/UDP"), port("0.0.0.0/8084/UDP"))),
                            false)))
            .add(false, true);
    }

    private static ContainerPort port(final String hostPort) {
        final String[] hostPortArr = hostPort.split("/");
        return new ContainerPort(null, hostPortArr[0], Integer.parseInt(hostPortArr[1]), null, hostPortArr[2]);
    }


    @CartesianProductTest(name = "{0}")
    @Disabled("Re-enable when we add topology-spread constraints again")
    public void testPodTopologySpread(final TestArguments args, final boolean scope) {
        // Unpack arguments
        final int numNodes = (int) args.get(1);
        final List<List<TopologySpreadConstraint>> pendingPods = (List<List<TopologySpreadConstraint>>) args.get(2);
        final Predicate<List<String>> assertOn = (Predicate<List<String>>) args.get(3);
        final boolean feasible = (boolean) args.get(4);

        final List<String> policies = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                Policies.disallowNullNodeSoft(),
                Policies.podTopologySpreadConstraints());
        final Iterator<List<TopologySpreadConstraint>> pendingPodsIt = pendingPods.iterator();
        final var result = TestScenario.withPolicies(policies, scope)
                .withNodeGroup("n", numNodes, node -> {
                    node.getMetadata().setLabels(Map.of("some-label", node.getMetadata().getName() + "-label",
                                                        "other-label", node.getMetadata().getName() + "-label"));
                })
                .withPodGroup("newPod", pendingPods.size(),
                        pod -> {
                            pod.getSpec().setTopologySpreadConstraints(pendingPodsIt.next());
                            pod.getMetadata().setLabels(Map.of("k1", "v1"));
                        })
                .runInitialPlacement();
        if (feasible) {
            result.expect(nodesForPodGroup("newPod"), assertOn);
        } else {
            result.expect(nodesForPodGroup("newPod"), EQUALS, nodeGroup("NULL_NODE"));
        }
    }

    @SuppressWarnings("UnusedMethod")
    private static CartesianProductTest.Sets testPodTopologySpread() {
        final Predicate<List<String>> onePodPerNode = nodes -> nodes.size() == Set.copyOf(nodes).size();
        final Predicate<List<String>> twoPerNode = nodes -> {
            final Map<String, Long> counts = nodes.stream().collect(groupingBy(Function.identity(), counting()));
            return new HashSet<>(counts.values()).equals(Set.of(2L));
        };
        return new CartesianProductTest.Sets()
            .add(
                new TestArguments(Arrays.asList("Pods should be placed on separate nodes", 5,
                                  List.of(List.of(spread(1)),
                                          List.of(spread(1))),
                                  onePodPerNode, true)),
                new TestArguments(Arrays.asList("Pods should be evenly distributed across two nodes", 2,
                        List.of(List.of(spread(1)),
                                List.of(spread(1)),
                                List.of(spread(1)),
                                List.of(spread(1))),
                         twoPerNode, true)))
            .add(false, true);
    }

    private static TopologySpreadConstraint spread(final int maxSkew) {
        final TopologySpreadConstraint c = new TopologySpreadConstraint();
        c.setMaxSkew(maxSkew);
        c.setTopologyKey("some-label");
        c.setWhenUnsatisfiable("DoNotSchedule");
        final LabelSelector l = new LabelSelector();
        l.setMatchLabels(Map.of("k1", "v1"));
        c.setLabelSelector(l);
        return c;
    }

    /*
     * Use this to test Scheduler dumps
     */
    @Test
    @Disabled
    public void testSchedulerDebugDump() {
        //final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DDlogDBConnectionPool dbConnectionPool = DDlogDBConnectionPool.create(null, false);
        dbConnectionPool.buildDDlog(false);

        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        DebugUtils.dbLoad(conn, "<enter some valid value here>");
        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool).setDebugMode(true).build();
        System.out.println(conn.selectFrom("PODS_TO_ASSIGN").fetch());
        System.out.println(conn.selectFrom("POD_RESOURCE_DEMANDS").fetch());
        System.out.println(conn.selectFrom("NODE_RESOURCES").fetch().formatCSV());
        System.out.println(conn.selectFrom("NODE_LABELS").fetch());
        System.out.println(conn.selectFrom("NODES_THAT_HAVE_TOLERATIONS").fetch());
        System.out.println(conn.selectFrom("POD_NODE_SELECTOR_MATCHES").fetch());
        System.out.println(conn.selectFrom("POD_NODE_SELECTOR_LABELS").fetch());
        System.out.println(conn.selectFrom("MATCH_EXPRESSIONS").fetch());
        System.out.println(conn.selectFrom("MATCHING_NODES").fetch());
        System.out.println(conn.selectFrom("POD_LABELS").fetch());
        System.out.println(conn.selectFrom("POD_INFO").fetch());
        System.out.println(conn.selectFrom("SPARE_CAPACITY_PER_NODE").fetch());
        final Result<? extends Record> results = scheduler.initialPlacement();
        System.out.println(results);
    }

    /*
     * Make sure that autoscope gets topK for each node resource
     */
    @CartesianProductTest(name = "{0}")
    public void testAutoScopeTopK(final TestArguments args) {

        final IConnectionPool dbConnectionPool = setupDDlog();

        final List<Integer> nodeCpuCapacities = (List<Integer>) args.get(1);
        final List<Integer> nodeMemoryCapacities = (List<Integer>) args.get(2);
        final List<Integer> nodeStorageCapacities = (List<Integer>) args.get(3);
        final List<Integer> nodepodsCapacities = (List<Integer>) args.get(4);
        final Iterator<Integer> cpuCapIt = nodeCpuCapacities.iterator();
        final Iterator<Integer> memCapIt = nodeMemoryCapacities.iterator();
        final Iterator<Integer> storageCapIt = nodeStorageCapacities.iterator();
        final Iterator<Integer> podsCapIt = nodepodsCapacities.iterator();
        final HashMap<String, List<Integer>> expected = new HashMap<>();
        expected.put("cpu", (List<Integer>) args.get(5));
        expected.put("memory", (List<Integer>) args.get(6));
        expected.put("ephemeral-storage", (List<Integer>) args.get(7));
        expected.put("pods", (List<Integer>) args.get(8));
        Set<Integer> uniq = new HashSet<>();
        for (Map.Entry<String, List<Integer>> entry : expected.entrySet()) {
            uniq.addAll(entry.getValue());
        }

        final int limit = 3;
        final List<String> policies = Policies.getInitialPlacementPolicies();

        final var scenario = TestScenario.withPolicies(policies, limit, dbConnectionPool)
                .withNodeGroup("n", nodeCpuCapacities.size(), (node) -> {
                    node.getStatus().getCapacity().put("cpu",
                            new Quantity(String.valueOf(cpuCapIt.next())));
                    node.getStatus().getCapacity().put("memory",
                            new Quantity(String.valueOf(memCapIt.next())));
                    node.getStatus().getCapacity().put("ephemeral-storage",
                            new Quantity(String.valueOf(storageCapIt.next())));
                    node.getStatus().getCapacity().put("pods",
                            new Quantity(String.valueOf(podsCapIt.next())));
                })
                .build();
        final List<Record> topk = scenario.builder().getScope().getSortView();
        assertTrue(topk.size() == uniq.size() * 4);
        for (final Record r : topk) {
            final String resource = r.get("RESOURCE").toString();
            final Integer id = Integer.parseInt(r.get("NAME").toString().substring(2));
            assertTrue(uniq.contains(id));
        }
    }

    private static CartesianProductTest.Sets testAutoScopeTopK() {
        return new CartesianProductTest.Sets()
                .add(
                        new TestArguments(Arrays.asList("Top k with ties",
                                List.of(10, 10, 9, 9, 8, 8, 7, 7, 6, 6),
                                List.of(9, 9, 9, 10, 10, 5, 4, 3, 2, 1),
                                List.of(10, 9, 9, 9, 9, 9, 4, 3, 2, 1),
                                List.of(10, 9, 8, 7, 6, 5, 4, 3, 2, 1),
                                List.of(0, 1, 2),
                                List.of(2, 3, 4),
                                List.of(0, 1, 2),
                                List.of(0, 1, 2))),

                        new TestArguments(Arrays.asList("All equal",
                                List.of(10, 10, 10, 10, 10, 10, 10, 10, 10, 10),
                                List.of(10, 10, 10, 10, 10, 10, 10, 10, 10, 10),
                                List.of(10, 10, 10, 10, 10, 10, 10, 10, 10, 10),
                                List.of(10, 10, 10, 10, 10, 10, 10, 10, 10, 10),
                                List.of(0, 1, 2),
                                List.of(0, 1, 2),
                                List.of(0, 1, 2),
                                List.of(0, 1, 2))),

                        new TestArguments(Arrays.asList("Random order",
                                List.of(10, 9, 8, 7, 6, 5, 4, 3, 2, 1),
                                List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                                List.of(7, 6, 5, 10, 9, 8, 4, 3, 2, 1),
                                List.of(1, 10, 3, 4, 8, 6, 7, 5, 9, 2),
                                List.of(0, 1, 2),
                                List.of(9, 8, 7),
                                List.of(3, 4, 5),
                                List.of(1, 8, 4)))
                );
    }

    /*
     * Make sure that the scheduler places all pending pods even though it may attempt to place only a subset
     * of them at a time
     */
    @Test
    public void testPlaceAllPendingPodsRegardlessOfBatchSize() {
        final IConnectionPool dbConnectionPool = setupDDlog();

        final int numNodes = 50;
        final int numPods = 102;
        final List<String> policies = Policies.getInitialPlacementPolicies();
        final var result = TestScenario.withPolicies(policies, false, dbConnectionPool)
                .withNodeGroup("n", numNodes)
                .withPodGroup("pods", numPods)
                .build();
        result.scheduler().scheduleAllPendingPods(new EmulatedPodToNodeBinder(result.conn()));
        final Result<Record> fetch = result.conn().getConnectionToDb().fetch("select * from POD_INFO");
                                                  //.selectFrom(Tables.POD_INFO).fetch();

        fetch.forEach(x -> {
            final String nodeName = x.get("NODE_NAME", String.class);

            assertTrue(nodeName != null && nodeName.startsWith("n"));
        });
    }

    @Test
    public void testFilterNodes() {
        final var scenario = TestScenario.withPolicies(Policies.getInitialPlacementPolicies())
                .withNodeGroup("node", 8)
                .withPodGroup("pods", 10).build();
        // Schedule using a filter
        final DSLContext conn = scenario.conn().getConnectionToDb();
        final Result<? extends Record> results = scenario.scheduler().initialPlacement((table) -> {
            if (table.getName().equalsIgnoreCase("spare_capacity_per_node")) {
                final String lb = "node-" + 3;
                final String ub = "none-" + 7;
                return conn.selectFrom(table).where(field("name").ge(lb).and(field("name").le(ub))).fetch();
            }
            return conn.selectFrom(table).fetch();
        });
        assertEquals(10, results.size());
        final List<String> allowedNodes = new ArrayList<>(
                Arrays.asList("node-3", "node-4", "node-5", "node-6", "node-7"));
        results.forEach(r ->
                assertTrue(allowedNodes.contains(r.get("CONTROLLABLE__NODE_NAME", String.class))));
    }


    /*
     * Test preemption code path
     */
    @Test
    public void testPreemption() {
        final IConnectionPool pool = setupDDlog();
        final List<String> policiesInitial = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                                                    Policies.disallowNullNodeSoft(),
                                                    Policies.podAntiAffinityPredicate());
        final List<String> policiesPreemption = Policies.getInitialPlacementPolicies(Policies.nodePredicates(),
                Policies.disallowNullNodeSoft(),
                Policies.podAntiAffinityPredicate(),
                Policies.preemption());
        final var scenario = TestScenario.withPolicies(policiesInitial, policiesPreemption, pool)
                .withNodeGroup("node", 10, (node) -> {
                    node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(100)));
                    node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(100)));
                })
                .forSystemPods("node", (pod) -> {
                    final String nodeName = pod.getSpec().getNodeName();
                    final int i = Integer.parseInt(nodeName.split("-")[1]);
                    pod.getSpec().setPriority(i < 5 ? 20 : 150);
                    pod.getMetadata().setLabels(Map.of("k1", "l1"));
                })
                .withPodGroup("pod", 1, (pod) -> {
                    pod.getSpec().setPriority(100);
                    final List<PodAffinityTerm> inTerm = List.of(term("kubernetes.io/hostname",
                                                                          podExpr("k1", "In", "l1")));
                    final PodAntiAffinity podAntiAffinity = new PodAntiAffinity();
                    podAntiAffinity.setRequiredDuringSchedulingIgnoredDuringExecution(inTerm);
                    pod.getSpec().getAffinity().setPodAntiAffinity(podAntiAffinity);
                }).build();
        final Result<? extends Record> results = scenario.scheduler().initialPlacement();
        // No pods get assignments
        assertEquals(Set.of("NULL_NODE"), results.intoSet(field("CONTROLLABLE__NODE_NAME", String.class)));
        // Preemption must succeed for new pods
        final Result<? extends Record> preemption = scenario.scheduler().preempt();
        final Set<String> runningPodAssignments = new HashSet<>();
        preemption.forEach(
            r -> {
                final String assignment = r.get(field("CONTROLLABLE__NODE_NAME", String.class));
                if (r.get(field("POD_NAME", String.class)).startsWith("pod-")) {
                    assertNotEquals("NULL_NODE", assignment);
                } else {
                    // We should only consider nodes with lower priority pods
                    assertEquals(20, r.get("PRIORITY", Integer.class));
                    runningPodAssignments.add(assignment);
                }
            }
        );
        assertTrue(runningPodAssignments.contains("NULL_NODE"));
    }

    /*
     * Test requeue code path
     */
    @Test
    public void testRequeue() throws InterruptedException {
        final IConnectionPool dbConnectionPool = setupDDlog();

        final var scenario = TestScenario.withPolicies(Policies.getInitialPlacementPolicies(), false, dbConnectionPool)
                .withNodeGroup("node", 1, (node) -> {
                    node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(100)));
                })
                .withPodGroup("pods", 1, (pod) -> {
                    final Map<String, Quantity> resourceRequests = new HashMap<>();
                    resourceRequests.put("cpu", new Quantity("101"));
                    pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
                }).build();
        final var conn = scenario.conn().getConnectionToDb();
        assertEquals(1, conn.fetch("SELECT * FROM PODS_TO_ASSIGN_NO_LIMIT").size());
        final var binder = new EmulatedPodToNodeBinder(scenario.conn());
        // Requeued pod should only be considered once every second
        for (int i = 0; i < 2; i++) {
            scenario.scheduler().scheduleAllPendingPods(binder);
            assertEquals(0, conn.fetch("SELECT * FROM PODS_TO_ASSIGN_NO_LIMIT").size());
            Thread.sleep(1000);
            PodEventsToDatabase.tick(conn).execute();
            assertEquals(1, conn.fetch("SELECT * FROM PODS_TO_ASSIGN_NO_LIMIT").size());
        }
    }

    static Map<String, String> map(final String k1, final String v1) {
        return Collections.singletonMap(k1, v1);
    }

    static Map<String, String> map(final String k1, final String v1, final String k2, final String v2) {
        final Map<String, String> ret = new HashMap<>();
        ret.put(k1, v1);
        ret.put(k2, v2);
        return ret;
    }

    static NodeSelectorTerm term(final NodeSelectorRequirement... requirements) {
        return new NodeSelectorTermBuilder()
                .withMatchExpressions(requirements)
                .build();
    }

    static NodeSelectorRequirement nodeExpr(final String key, final String op, final String... values) {
        final NodeSelectorRequirement requirement = new NodeSelectorRequirement();
        requirement.setKey(key);
        requirement.setOperator(op);
        requirement.setValues(List.of(values));
        return requirement;
    }

    static PodAffinityTerm term(final String topologyKey, final LabelSelectorRequirement... requirements) {
        final LabelSelector labelSelector = new LabelSelectorBuilder()
                .withMatchExpressions(requirements)
                .build();
        return new PodAffinityTermBuilder()
                .withLabelSelector(labelSelector)
                .withTopologyKey(topologyKey)
                .build();
    }

    static LabelSelectorRequirement podExpr(final String key, final String op, final String... values) {
        final LabelSelectorRequirement requirement = new LabelSelectorRequirement();
        requirement.setKey(key);
        requirement.setOperator(op);
        requirement.setValues(List.of(values));
        return requirement;
    }

    static Pod newPod(final String name) {
        return newPod(name, "Pending");
    }

    static Pod newPod(final String name, final String status) {
        return newPod(name, UUID.randomUUID(), status, Collections.emptyMap(), Collections.emptyMap());
    }

    static Pod newPod(final String podName, final UUID uid, final String phase,
                      final Map<String, String> selectorLabels, final Map<String, String> labels) {
        final Pod pod = new Pod();
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp("1");
        meta.setNamespace("default");
        meta.setResourceVersion("0");
        final PodSpec spec = new PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        spec.setNodeSelector(selectorLabels);

        final Container container = new Container();
        container.setName("ignore");
        container.setImage("ignore");

        final ResourceRequirements resourceRequirements = new ResourceRequirements();
        resourceRequirements.setRequests(Collections.emptyMap());
        container.setResources(resourceRequirements);
        spec.getContainers().add(container);

        final Affinity affinity = new Affinity();
        final NodeAffinity nodeAffinity = new NodeAffinity();
        affinity.setNodeAffinity(nodeAffinity);
        spec.setAffinity(affinity);
        final PodStatus status = new PodStatus();
        status.setPhase(phase);
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }

    static Node newNode(final String nodeName, final Map<String, String> labels,
                        final List<NodeCondition> conditions) {
        return newNode(nodeName, UUID.randomUUID(), labels, conditions);
    }

    static Node newNode(final String nodeName, final UUID uid, final Map<String, String> labels,
                 final List<NodeCondition> conditions) {
        final Node node = new Node();
        final NodeStatus status = new NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("10", "m"));
        quantityMap.put("memory", new Quantity("1000"));
        quantityMap.put("ephemeral-storage", new Quantity("1000"));
        quantityMap.put("pods", new Quantity("100"));
        status.setCapacity(quantityMap);
        status.setAllocatable(quantityMap);
        status.setImages(Collections.emptyList());
        node.setStatus(status);
        status.setConditions(conditions);
        final NodeSpec spec = new NodeSpec();
        spec.setUnschedulable(false);
        spec.setTaints(Collections.emptyList());
        node.setSpec(spec);
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(nodeName);
        meta.setLabels(labels);
        node.setMetadata(meta);
        return node;
    }
}
