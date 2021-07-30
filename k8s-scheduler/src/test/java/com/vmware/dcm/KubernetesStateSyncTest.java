/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KubernetesStateSyncTest {
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
        server = new KubernetesServer(false, true);
        server.before();
        assertNotNull(server);
        final int numPods = 3;
        final NamespacedKubernetesClient client = server.getClient();
        final Node node = SchedulerTest.newNode("n1", Collections.emptyMap(), Collections.emptyList());
        final DBConnectionPool conn = new DBConnectionPool();
        final KubernetesStateSync stateSync = new KubernetesStateSync(client);
        final CountDownLatch latch = new CountDownLatch(numPods);
        final PodEventsToDatabase podEventsToDatabase = new PodEventsToDatabase(conn);
        stateSync.setupInformersAndPodEventStream(conn, (pe) -> {
            podEventsToDatabase.handle(pe);
            latch.countDown();
        });
        stateSync.startProcessingEvents();
        client.nodes().create(node);
        IntStream.range(0, numPods).forEach(i -> {
                final PodBuilder podBuilder = new PodBuilder().withNewMetadata()
                        .withName("pod" + i)
                        .withNamespace("test")
                        .endMetadata()
                        .withNewSpec()
                        .withSchedulerName(Scheduler.SCHEDULER_NAME)
                        .endSpec()
                        .withNewStatus()
                        .withPhase("Pending")
                        .endStatus();
                client.pods().create(podBuilder.build());
            }
        );
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

        final Scheduler scheduler = new Scheduler(conn, false, 4);

        final KubernetesBinder binder = new KubernetesBinder(client);
        scheduler.startScheduler(binder, 50, 100);

        // Create pod event and wait for scheduler to create a binding
        scheduler.handlePodEvent(new PodEvent(PodEvent.Action.ADDED, pod));

        int attempts = 5;
        boolean found = false;
        while (attempts > 0) {
            Thread.sleep(1000);

            // Test whether the scheduler created a binding. This call returns null if the binding does not exist.
            final Binding binding = client.bindings().inNamespace("default")
                                                     .withName(pod.getMetadata().getName()).get();
            if (binding != null) {
                found = true;
                break;
            }
            attempts--;
        }
        assertTrue(found);
    }
}
