/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ITBase {
    static final String TEST_NAMESPACE = "default"; // make sure that pod-specs use this namespace

    static DefaultKubernetesClient fabricClient;

    @BeforeAll
    public static void setupConnectionAndCreateNamespace() {
        fabricClient = new DefaultKubernetesClient();

        // Create new namespace if required
        final Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(TEST_NAMESPACE).endMetadata()
                .build();
        fabricClient.namespaces().createOrReplace(namespace);
    }

    @BeforeEach
    @Timeout(60 /* seconds */)
    public void deleteAllRunningPods() throws Exception {
        final List<Deployment> deployments = fabricClient.apps().deployments()
                                                         .inNamespace(TEST_NAMESPACE).list().getItems();
        for (final Deployment deployment: deployments) {
            fabricClient.apps().deployments()
                        .inNamespace(TEST_NAMESPACE).delete(deployment);
        }
        waitUntil((n) -> hasDrained());
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
