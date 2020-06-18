/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Creates pods that correspond to a deployment without actually deploying them to a real cluster. Allows us
 * to replay traces locally.
 */
public class EmulatedPodDeployer implements IPodDeployer {
    private static final Logger LOG = LoggerFactory.getLogger(EmulatedPodDeployer.class);
    private final PodResourceEventHandler resourceEventHandler;
    private final String namespace;
    private final Map<String, List<Pod>> pods = new ConcurrentHashMap<>();
    private final AtomicInteger uidCounter = new AtomicInteger(0);

    EmulatedPodDeployer(final PodResourceEventHandler podResourceEventHandler, final String namespace) {
        this.resourceEventHandler = podResourceEventHandler;
        this.namespace = namespace;
    }

    @Override
    public Runnable startDeployment(final List<Pod> deployment) {
        return new StartDeployment(deployment);
    }

    @Override
    public Runnable endDeployment(final List<Pod> deployment) {
        return new EndDeployment(deployment);
    }

    private class StartDeployment implements Runnable {
        List<Pod> deployment;

        StartDeployment(final List<Pod> dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            final Pod firstPodInDeployment = deployment.get(0);
            final String deploymentName = firstPodInDeployment.getMetadata().getName();
            LOG.info("Creating deployment (name:{}, schedulerName:{}, replicas:{}) at {}",
                    deploymentName, firstPodInDeployment.getSpec().getSchedulerName(),
                    deployment.size(), System.currentTimeMillis());

            for (final Pod pod: deployment) {
                pod.getMetadata().setCreationTimestamp("" + System.currentTimeMillis());
                pod.getMetadata().setNamespace(namespace);
                pod.getMetadata().setResourceVersion("101");
                pod.getMetadata().setUid("" + uidCounter.incrementAndGet());
                final OwnerReference reference = new OwnerReference();
                reference.setName(deploymentName);
                pod.getMetadata().setOwnerReferences(List.of(reference));
                final PodSpec spec = pod.getSpec();
                final PodStatus status = new PodStatus();
                status.setPhase("Pending");
                pod.setSpec(spec);
                pod.setStatus(status);
                pods.computeIfAbsent(deploymentName, (k) -> new ArrayList<>()).add(pod);
                resourceEventHandler.onAdd(pod);
            }
        }
    }

    private class EndDeployment implements Runnable {
        List<Pod> deployment;

        EndDeployment(final List<Pod> dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            final Pod firstPodOfDeployment = deployment.get(0);
            LOG.info("Terminating deployment (name:{}, schedulerName:{}, replicas:{}) at {}",
                    firstPodOfDeployment.getMetadata().getName(), firstPodOfDeployment.getSpec().getSchedulerName(),
                    deployment.size(), System.currentTimeMillis());
            final List<Pod> podsList = pods.get(firstPodOfDeployment.getMetadata().getName());
            Preconditions.checkNotNull(podsList);
            for (final Pod pod: podsList) {
                resourceEventHandler.onDeleteSync(pod, false);
            }
        }
    }
}