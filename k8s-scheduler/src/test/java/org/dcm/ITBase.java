/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ITBase {
    private static final String K8S_URL_PROPERTY = "k8sUrl";
    static final String TEST_NAMESPACE = "default"; // make sure that pod-specs use this namespace

    static CoreV1Api coreV1Api;
    static DefaultKubernetesClient fabricClient;

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
    @Timeout(60 /* seconds */)
    public void deleteAllRunningPods() throws Exception {
        fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE).delete();
        waitUntil((n) -> hasDrained());
        Thread.sleep(10000); // wait 10 seconds to rest infrastructure before starting test
    }


    Deployment launchDeploymentFromFile(final String resourceName) {
        return launchDeploymentFromFile(resourceName, "default-scheduler");
    }

    Deployment launchDeploymentFromFile(final String resourceName, final String schedulerName) {
        final URL url = getClass().getClassLoader().getResource(resourceName);
        assertNotNull(url);
        final File file = new File(url.getFile());
        final Deployment deployment = fabricClient.apps().deployments().load(file).get();
        deployment.getSpec().getTemplate().getSpec().setSchedulerName(schedulerName);
        fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE)
                .create(deployment);
        return deployment;
    }

    boolean hasNRunningPods(final int numPods) {
        return fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems()
                .stream().filter(e -> e.getStatus().getPhase().equals("Running"))
                .count() == numPods;
    }

    void waitUntil(final Predicate<Integer> condition) throws Exception {
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
