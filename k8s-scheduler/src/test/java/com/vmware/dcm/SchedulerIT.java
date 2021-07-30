/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import com.vmware.dcm.trace.TraceReplayer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class is used to run integration tests in SchedulerIT against a real
 * Kubernetes cluster. To invoke this test, run the following in the
 * commandline:
 *
 * $: mvn integration-test -DargLine="-Dk8sUrl=http://<hostname>:<port>"
 *
 * where http://<hostname>:<port> points to a Kubernetes API endpoint.
 */
public class SchedulerIT extends ITBase {

    @Tag("integration-test")
    @Test()
    @Timeout(60 /* seconds */)
    public void testDeployments() throws Exception {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final Scheduler scheduler = new Scheduler(dbConnectionPool, true, 4);
        final KubernetesStateSync stateSync = new KubernetesStateSync(fabricClient);

        stateSync.setupInformersAndPodEventStream(dbConnectionPool, scheduler::handlePodEvent);
        scheduler.startScheduler(new KubernetesBinder(fabricClient), 50, 1000);
        stateSync.startProcessingEvents();

        // Add a new one
        final URL url = getClass().getClassLoader().getResource("no-constraints.yml");
        assertNotNull(url);
        final File file = new File(url.getFile());
        final Deployment deployment = fabricClient.apps().deployments().load(file).get();
        fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE)
                    .create(deployment);

        final int newPodsToCreate = deployment.getSpec().getReplicas();
        waitUntil(fabricClient, (n) -> hasNRunningPods(newPodsToCreate));
        final List<Pod> items =
                fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems();
        assertEquals(newPodsToCreate, items.size());
        items.forEach(pod -> assertNotEquals("kube-master", pod.getSpec().getNodeName()));
        stateSync.shutdown();
        scheduler.shutdown();
    }

    @Tag("integration-test")
    @Test()
    @Timeout(60 /* seconds */)
    public void testAffinityAntiAffinity() throws Exception {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final Scheduler scheduler = new Scheduler(dbConnectionPool, true, 4);
        final KubernetesStateSync stateSync = new KubernetesStateSync(fabricClient);

        stateSync.setupInformersAndPodEventStream(dbConnectionPool, scheduler::handlePodEvent);
        scheduler.startScheduler(new KubernetesBinder(fabricClient),  50, 1000);
        stateSync.startProcessingEvents();

        // Add a new one
        final Deployment cacheExample = launchDeploymentFromFile("cache-example.yml",
                                                                 Scheduler.SCHEDULER_NAME);
        final String cacheName = cacheExample.getMetadata().getName();
        final Deployment webStoreExample = launchDeploymentFromFile("web-store-example.yml",
                                                                    Scheduler.SCHEDULER_NAME);
        final String webStoreName = webStoreExample.getMetadata().getName();

        final int newPodsToCreate = cacheExample.getSpec().getReplicas() + webStoreExample.getSpec().getReplicas();
        waitUntil(fabricClient, (n) -> hasNRunningPods(newPodsToCreate));
        final List<Pod> items = fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems();
        assertEquals(newPodsToCreate, items.size());
        items.forEach(pod -> assertNotEquals("kube-master", pod.getSpec().getNodeName()));

        final Map<String, List<String>> podsByNode = new HashMap<>();

        items.forEach(pod -> podsByNode.computeIfAbsent(pod.getSpec().getNodeName(), k -> new ArrayList<>())
                                       .add(pod.getMetadata().getName()));
        podsByNode.forEach((nodeName, pods) -> {
            assertEquals(2, pods.size());
            assertTrue(pods.stream().anyMatch(p -> p.contains(webStoreName)));
            assertTrue(pods.stream().anyMatch(p -> p.contains(cacheName)));
        });
        stateSync.shutdown();
        scheduler.shutdown();
    }


    @Tag("integration-test")
    @Test()
    @Timeout(120 /* seconds */)
    public void testSmallTrace() throws Exception {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final Scheduler scheduler = new Scheduler(dbConnectionPool, true, 4);
        final KubernetesStateSync stateSync = new KubernetesStateSync(fabricClient);

        stateSync.setupInformersAndPodEventStream(dbConnectionPool, scheduler::handlePodEvent);
        scheduler.startScheduler(new KubernetesBinder(fabricClient), 50, 1000);
        stateSync.startProcessingEvents();

        // Add a new one
        final KubernetesPodDeployer deployer = new KubernetesPodDeployer(fabricClient, "default");
        final TraceReplayer traceReplayer = new TraceReplayer();
        traceReplayer.runTrace(fabricClient, "test-data.txt", deployer, "dcm-scheduler",
                100, 50, 100, 1000000);
        stateSync.shutdown();
        scheduler.shutdown();
    }

}
