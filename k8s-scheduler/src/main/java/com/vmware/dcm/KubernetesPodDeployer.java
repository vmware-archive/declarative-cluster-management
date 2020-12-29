/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Deploys and deletes pods against an actual Kubernetes cluster
 */
public class KubernetesPodDeployer implements IPodDeployer {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesPodDeployer.class);

    private final NamespacedKubernetesClient fabricClient;
    private final String namespace;

    public KubernetesPodDeployer(final NamespacedKubernetesClient fabricClient, final String namespace) {
        this.fabricClient = fabricClient;
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
        final List<Pod> deployment;

        StartDeployment(final List<Pod> dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            final Pod firstPodInDeployment = deployment.get(0);
            LOG.info("Creating deployment (name:{}, schedulerName:{}, replicas:{}) with masterUrl {} at {}",
                    firstPodInDeployment.getMetadata().getName(), firstPodInDeployment.getSpec().getSchedulerName(),
                    deployment.size(), fabricClient.getConfiguration().getMasterUrl(),
                    System.currentTimeMillis());
            deployment.forEach(p -> fabricClient.pods().inNamespace(namespace).create(p));
        }
    }

    private class EndDeployment implements Runnable {
        final List<Pod> deployment;

        EndDeployment(final List<Pod> dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            final Pod firstPodInDeployment = deployment.get(0);
            LOG.info("Terminating deployment (name:{}, schedulerName:{}) with masterUrl {} at {}",
                    firstPodInDeployment.getMetadata().getName(), firstPodInDeployment.getSpec().getSchedulerName(),
                    fabricClient.getConfiguration().getMasterUrl(), System.currentTimeMillis());
            fabricClient.pods().inNamespace(namespace).delete(deployment);
        }
    }
}