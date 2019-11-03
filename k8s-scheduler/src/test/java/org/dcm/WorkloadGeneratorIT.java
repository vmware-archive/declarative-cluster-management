/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkloadGeneratorIT extends ITBase {

    @Test
    public void testAffinityAntiAffinity() throws Exception {
        // Add a new one
        final Deployment cacheExample = launchDeploymentFromFile("cache-example.yml");
        final String cacheName = cacheExample.getMetadata().getName();
        final Deployment webStoreExample = launchDeploymentFromFile("web-store-example.yml");
        final String webStoreName = webStoreExample.getMetadata().getName();

        final int newPodsToCreate = cacheExample.getSpec().getReplicas() + webStoreExample.getSpec().getReplicas();
        waitUntil((n) -> hasNRunningPods(newPodsToCreate));
        final List<Pod> pods = fabricClient.pods().inNamespace(TEST_NAMESPACE).list().getItems();
        assertEquals(newPodsToCreate, pods.size());

        pods.forEach(pod -> assertNotEquals(pod.getSpec().getNodeName(), "kube-master"));

        final Map<String, List<String>> podsByNode = new HashMap<>();

        pods.forEach(pod -> podsByNode.computeIfAbsent(pod.getSpec().getNodeName(), k -> new ArrayList<>())
                .add(pod.getMetadata().getName()));
        podsByNode.forEach((nodeName, podsAssignedToNode) -> {
            assertEquals(2, podsAssignedToNode.size());
            assertTrue(podsAssignedToNode.stream().anyMatch(p -> p.contains(webStoreName)));
            assertTrue(podsAssignedToNode.stream().anyMatch(p -> p.contains(cacheName)));
        });
    }
}