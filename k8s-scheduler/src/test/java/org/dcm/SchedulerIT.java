/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.util.Config;
import io.reactivex.Flowable;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

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
public class SchedulerIT {
    private static final String TEST_NAMESPACE = "default"; // make sure that pod-specs use this namespace
    private static final String K8S_URL_PROPERTY = "k8sUrl";

    private static CoreV1Api coreV1Api;
    private static DefaultKubernetesClient fabricClient;

    @BeforeAll
    public static void setupConnectionAndCreateNamespace() {
        final String k8sUrl = System.getProperty(K8S_URL_PROPERTY);
        assertNotNull(k8sUrl,
           "k8sUrl system property has not been configured. If running this integration test " +
                    "through maven, please run: " +
                    "\n $: mvn integration-test -DargLine=\"-Dk8sUrl=http://<hostname>:<port>\"");
        final ApiClient client = Config.fromUrl(k8sUrl);
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
        Configuration.setDefaultApiClient(client);
        coreV1Api = new CoreV1Api();
        final io.fabric8.kubernetes.client.Config config =
                new ConfigBuilder().withMasterUrl(k8sUrl).build();
        fabricClient = new DefaultKubernetesClient(config);

        // Create new namespace if required
        final Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(TEST_NAMESPACE).endMetadata()
                                                           .build();
        fabricClient.namespaces().createOrReplace(namespace);
    }

    @BeforeEach
    @Timeout(30 /* seconds */)
    public void deleteAllRunningPods() throws Exception {
        fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE).delete();
        waitUntil((n) -> hasDrained());
    }

    @Test()
    @Timeout(60 /* seconds */)
    public void testDeployments() throws Exception {
        final DSLContext conn = Scheduler.setupDb();
        final Scheduler scheduler = new Scheduler(conn, Policies.getDefaultPolicies(), "CHUFFED", true, "");
        final KubernetesStateSync stateSync = new KubernetesStateSync(fabricClient);

        final Flowable<List<PodEvent>> eventStream =
                stateSync.setupInformersAndPodEventStream(conn, 50, 1000);
        scheduler.startScheduler(eventStream, coreV1Api);
        stateSync.startProcessingEvents();

        // Add a new one
        final URL url = getClass().getClassLoader().getResource("no-constraints.yml");
        assertNotNull(url);
        final File file = new File(url.getFile());
        final Deployment deployment = fabricClient.apps().deployments().load(file).get();
        fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE)
                    .create(deployment);

        final int newPodsToCreate = deployment.getSpec().getReplicas();
        waitUntil((n) -> hasNRunningPods(newPodsToCreate));
        final List<Pod> items = fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems();
        assertEquals(newPodsToCreate, items.size());
        items.forEach(pod -> assertNotEquals(pod.getSpec().getNodeName(), "kube-master"));
        stateSync.shutdown();
        scheduler.shutdown();
    }

    @Test()
    @Timeout(60 /* seconds */)
    public void testAffinityAntiAffinity() throws Exception {
        final DSLContext conn = Scheduler.setupDb();
        final Scheduler scheduler = new Scheduler(conn, Policies.getDefaultPolicies(), "CHUFFED", true, "");
        final KubernetesStateSync stateSync = new KubernetesStateSync(fabricClient);

        final Flowable<List<PodEvent>> eventStream =
                stateSync.setupInformersAndPodEventStream(conn, 50, 1000);
        scheduler.startScheduler(eventStream, coreV1Api);
        stateSync.startProcessingEvents();

        // Add a new one
        final Deployment cacheExample = launchDeploymentFromFile("cache-example.yml");
        final String cacheName = cacheExample.getMetadata().getName();
        final Deployment webStoreExample = launchDeploymentFromFile("web-store-example.yml");
        final String webStoreName = webStoreExample.getMetadata().getName();

        final int newPodsToCreate = cacheExample.getSpec().getReplicas() + webStoreExample.getSpec().getReplicas();
        waitUntil((n) -> hasNRunningPods(newPodsToCreate));
        final List<Pod> items = fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems();
        assertEquals(newPodsToCreate, items.size());
        items.forEach(pod -> assertNotEquals(pod.getSpec().getNodeName(), "kube-master"));

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

    private Deployment launchDeploymentFromFile(final String resourceName) {
        final URL url = getClass().getClassLoader().getResource(resourceName);
        assertNotNull(url);
        final File file = new File(url.getFile());
        final Deployment deployment = fabricClient.apps().deployments().load(file).get();
        fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE)
                .create(deployment);
        return deployment;
    }

    private void waitUntil(final Predicate<Integer> condition) throws Exception {
        if (condition.test(0)) {
            return;
        }
        final CountDownLatch latch = new CountDownLatch(1);
        fabricClient.pods().inNamespace(TEST_NAMESPACE).watch(new PodConditionWatcher(condition, latch));
        latch.await();
    }

    private boolean hasDrained() {
        return fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems().size() == 0;
    }

    private boolean hasNRunningPods(final int numPods) {
        return fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems()
                           .stream().filter(e -> e.getStatus().getPhase().equals("Running"))
                           .count() == numPods;
    }

    private static class PodConditionWatcher implements Watcher<Pod> {

        private final Predicate<Integer> condition;
        private final CountDownLatch latch;

        public PodConditionWatcher(final Predicate<Integer> condition, final CountDownLatch latch) {
            this.condition = condition;
            this.latch = latch;
        }

        @Override
        public void eventReceived(final Watcher.Action action, final Pod pod) {
            if (condition.test(0)) {
                latch.countDown();
            }
        }

        @Override
        public void onClose(final KubernetesClientException cause) {
        }
    }
}
