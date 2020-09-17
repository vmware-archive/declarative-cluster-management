/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeListBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.api.model.WatchEventBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KubernetesStateSyncTest {
    private static final Status OUTDATED_STATUS = new StatusBuilder().withCode(HttpURLConnection.HTTP_GONE)
            .withMessage("401: The event in requested index is outdated and cleared")
            .build();
    private static final WatchEvent OUTDATED_EVENT = new WatchEventBuilder().withStatusObject(OUTDATED_STATUS).build();
    @Nullable private KubernetesServer server;

    @AfterEach
    void tearDown() {
        assertNotNull(server);
        server.after();
    }

    /**
     * Tests our use of informers to subscribe to learn about pod and node events via the K8s API
     */
    @Test
    public void testKubernetesInformers() throws InterruptedException {
        server = new KubernetesServer(false);
        server.before();
        final String rv1 = "10";
        final String rv2 = "11";
        final String rv3 = "12";
        final String rv4 = "13";
        assertNotNull(server);
        final NamespacedKubernetesClient client = server.getClient();
        final Node node = SchedulerTest.newNode("n1", Collections.emptyMap(), Collections.emptyList());
        final PodBuilder podBuilder = new PodBuilder().withNewMetadata()
                                                         .withName("pod1")
                                                         .withNamespace("test")
                                                         .withCreationTimestamp("10")
                                                         .withResourceVersion(rv2)
                                                      .endMetadata()
                                                      .withNewSpec()
                                                         .withSchedulerName(Scheduler.SCHEDULER_NAME)
                                                      .endSpec()
                                                      .withNewStatus()
                                                         .withPhase("Pending")
                                                      .endStatus();
        server.expect().get()
                .withPath("/api/v1/namespaces/test/nodes")
                .andReturn(200, new NodeListBuilder().withNewMetadata()
                        .withResourceVersion(rv1).endMetadata().withItems(node).build()).once();
        server.expect().get()
                .withPath("/api/v1/namespaces/test/nodes?resourceVersion=" + rv1 + "&watch=true")
                .andUpgradeToWebSocket()
                .open()
                .waitFor(10)
                .andEmit(OUTDATED_EVENT).done().always();
        server.expect().get().withPath("/api/v1/namespaces/test/pods")
                .andReturn(200, new PodListBuilder().withNewMetadata()
                                                                 .withResourceVersion(rv1)
                                                              .endMetadata()
                                                              .withItems(Collections.emptyList()).build()).once();
        server.expect().get().withPath("/api/v1/namespaces/test/pods?resourceVersion=" + rv1 + "&watch=true")
                .andUpgradeToWebSocket()
                .open()
                .waitFor(10)
                .andEmit(new WatchEvent(podBuilder.editMetadata().withResourceVersion(rv2).endMetadata().build(),
                                        "ADDED"))
                .done().always();
        server.expect().get().withPath("/api/v1/namespaces/test/pods?resourceVersion=" + rv2 + "&watch=true")
                .andUpgradeToWebSocket()
                .open()
                .waitFor(10)
                .andEmit(new WatchEvent(podBuilder.editMetadata().withResourceVersion(rv3).endMetadata().build(),
                                        "MODIFIED"))
                .done().always();
        server.expect().get().withPath("/api/v1/namespaces/test/pods?resourceVersion=" + rv3 + "&watch=true")
                .andUpgradeToWebSocket()
                .open()
                .waitFor(10)
                .andEmit(new WatchEvent(podBuilder.editMetadata().withResourceVersion(rv4).endMetadata().build(),
                                        "DELETED"))
                .waitFor(40)
                .andEmit(OUTDATED_EVENT).done().always();

        final DBConnectionPool conn = new DBConnectionPool();
        final KubernetesStateSync stateSync = new KubernetesStateSync(client);
        final CountDownLatch latch = new CountDownLatch(3);
        final PodEventsToDatabase podEventsToDatabase = new PodEventsToDatabase(conn);
        stateSync.setupInformersAndPodEventStream(conn, (pe) -> {
            podEventsToDatabase.handle(pe);
            latch.countDown();
        });
        stateSync.startProcessingEvents();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        stateSync.shutdown();
    }


    /**
     * Given a single pod and node, test the Scheduler loop that pulls in events from
     * the notification queue, makes a placement decision, and creates the subsequent
     * pod binding.
     */
    @Test
    public void testSchedulerLoopAndBind() throws InterruptedException {
        server = new KubernetesServer(false, true);
        server.before();
        assertNotNull(server);
        final NamespacedKubernetesClient client = server.getClient();
        final Node node = SchedulerTest.newNode("n1", Collections.emptyMap(), Collections.emptyList());
        final Pod pod = SchedulerTest.newPod("p1");
        final DBConnectionPool conn = new DBConnectionPool();

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(conn);
        final PodResourceEventHandler podHandler = new PodResourceEventHandler(eventHandler::handle);
        final NodeResourceEventHandler nodeHandler = new NodeResourceEventHandler(conn);

        nodeHandler.onAddSync(node);
        podHandler.onAddSync(pod);

        final Scheduler scheduler = new Scheduler(conn,
                Policies.getDefaultPolicies(), "ORTOOLS", false, 4);

        final KubernetesBinder binder = new KubernetesBinder(client);
        scheduler.startScheduler(binder, 50, 100);

        // Create pod event and wait for scheduler to create a binding
        scheduler.handlePodEvent(new PodEvent(PodEvent.Action.ADDED, pod));
        Thread.sleep(1000);

        // Test whether the scheduler created a binding. This call returns null if the binding does not exist.
        final Binding binding = client.bindings().inNamespace("default").withName(pod.getMetadata().getName()).get();
        assertNotNull(binding);
    }
}
