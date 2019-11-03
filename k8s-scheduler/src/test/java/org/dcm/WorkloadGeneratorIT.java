/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * To run these specific tests, pass a `schedulerName` property to maven, for example:
 *
 *  mvn integrate-test -DargLine="-Dk8sUrl=<hostname>:<port> -DschedulerName=dcm-scheduler"
 */
public class WorkloadGeneratorIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(WorkloadGeneratorIT.class);
    private static final String SCHEDULER_NAME_PROPERTY = "schedulerName";
    @Nullable private static String schedulerName;

    @BeforeAll
    public static void setSchedulerFromEnvironment() {
        schedulerName = System.getProperty(SCHEDULER_NAME_PROPERTY);
    }

    @Test
    public void testAffinityAntiAffinity() throws Exception {
        assertNotNull(schedulerName);
        LOG.info("Running testAffinityAntiAffinity with parameters: MasterUrl:{} SchedulerName:{}",
                 fabricClient.getConfiguration().getMasterUrl(), schedulerName);

        // Add a new one
        final Deployment cacheExample = launchDeploymentFromFile("cache-example.yml", schedulerName);
        final String cacheName = cacheExample.getMetadata().getName();
        final Deployment webStoreExample = launchDeploymentFromFile("web-store-example.yml",
                                                                    schedulerName);
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