/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import com.vmware.dcm.trace.TraceReplayer;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WorkloadGeneratorTest {

    @Test
    public void testKubernetesDeployer() {
        final KubernetesServer server = new KubernetesServer(false, true);
        server.before();
        final NamespacedKubernetesClient client = server.getClient();
        final IPodDeployer deployer = new KubernetesPodDeployer(server.getClient(), "default");
        final Pod pod1 = SchedulerTest.newPod("pod1");
        final Pod pod2 = SchedulerTest.newPod("pod2");

        // Launch pods via deployer and verify whether they're created
        deployer.startDeployment(List.of(pod1, pod2)).run();
        final Pod pod1Result = client.pods().inNamespace("default").withName(pod1.getMetadata().getName()).get();
        Assert.assertEquals(pod1Result, pod1);
        final Pod pod2Result = client.pods().inNamespace("default").withName(pod2.getMetadata().getName()).get();
        Assert.assertEquals(pod2Result, pod2);

        // Delete pods via deployer and verify whether they're created
        deployer.endDeployment(List.of(pod1, pod2)).run();
        Assert.assertNull(client.pods().inNamespace("default").withName(pod1.getMetadata().getName()).get());
        Assert.assertNull(client.pods().inNamespace("default").withName(pod2.getMetadata().getName()).get());
    }

    @Test
    public void testSmallTrace() throws Exception {
        final KubernetesServer server = new KubernetesServer(false, true);
        server.before();
        final NamespacedKubernetesClient client = server.getClient();
        final IPodDeployer deployer = new KubernetesPodDeployer(client, "default");
        final TraceReplayer traceReplayer = new TraceReplayer();

        traceReplayer.runTrace(client, "test-data.txt", deployer, "default-scheduler",
                1, 1, 100, 1000, 100, 2);
        Assert.assertEquals(8, server.getMockServer().getRequestCount());
        final List<String> events = IntStream.range(0, 8)
                .mapToObj(e -> {
                    try {
                        return server.getMockServer().takeRequest().getMethod();
                    } catch (final InterruptedException interruptedException) {
                        return null;
                    }
                }).collect(Collectors.toList());
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(events.get(i), "POST");
        }
        for (int i = 4; i < 8; i++) {
            Assert.assertEquals(events.get(i), "DELETE");
        }
    }
}
