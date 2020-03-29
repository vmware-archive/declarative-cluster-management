/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Creates pods that correspond to a deployment without actually deploying them to a real cluster. Allows us
 * to replay traces locally.
 */
public class EmulatedPodDeployer implements IPodDeployer {
    private static final Logger LOG = LoggerFactory.getLogger(EmulatedPodDeployer.class);
    private final PodResourceEventHandler resourceEventHandler;
    private final String namespace;
    private final Map<String, List<Pod>> pods = new ConcurrentHashMap<>();

    EmulatedPodDeployer(final PodResourceEventHandler podResourceEventHandler, final String namespace) {
        this.resourceEventHandler = podResourceEventHandler;
        this.namespace = namespace;
    }

    @Override
    public Runnable startDeployment(final Deployment deployment) {
        return new StartDeployment(deployment);
    }

    @Override
    public Runnable endDeployment(final Deployment deployment) {
        return new EndDeployment(deployment);
    }

    private class StartDeployment implements Runnable {
        Deployment deployment;

        StartDeployment(final Deployment dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            final String deploymentName = deployment.getMetadata().getName();
            LOG.info("Creating deployment (name:{}, schedulerName:{}) at {}",
                    deploymentName, deployment.getSpec().getTemplate().getSpec().getSchedulerName(),
                    System.currentTimeMillis());

            for (int i = 0; i < deployment.getSpec().getReplicas(); i++) {
                final Pod pod = new Pod();
                final ObjectMeta meta = new ObjectMeta();
                meta.setName(deploymentName + "-" + i);
                meta.setCreationTimestamp("" + System.currentTimeMillis());
                meta.setNamespace(namespace);
                final OwnerReference reference = new OwnerReference();
                reference.setName(deploymentName);
                meta.setOwnerReferences(List.of(reference));
                final PodSpec spec = deployment.getSpec().getTemplate().getSpec();
                final PodStatus status = new PodStatus();
                status.setPhase("Pending");
                pod.setMetadata(meta);
                pod.setSpec(spec);
                pod.setStatus(status);
                pods.computeIfAbsent(deploymentName, (k) -> new ArrayList<>()).add(pod);
                resourceEventHandler.onAddSync(pod);
            }
        }
    }

    private class EndDeployment implements Runnable {
        Deployment deployment;

        EndDeployment(final Deployment dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            LOG.info("Terminating deployment (name:{}, schedulerName:{}) at {}",
                deployment.getMetadata().getName(), deployment.getSpec().getTemplate().getSpec().getSchedulerName(),
                System.currentTimeMillis());
            final List<Pod> podsList = pods.get(deployment.getMetadata().getName());
            Preconditions.checkNotNull(podsList);
            for (final Pod pod: podsList) {
                resourceEventHandler.onDeleteSync(pod, false);
            }
        }
    }
}