/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Deploys and deletes pods against an actual Kubernetes cluster
 */
public class KubernetesPodDeployer implements IPodDeployer {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesPodDeployer.class);

    private final DefaultKubernetesClient fabricClient;
    private final String namespace;

    public KubernetesPodDeployer(final DefaultKubernetesClient fabricClient, final String namespace) {
        this.fabricClient = fabricClient;
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
            LOG.info("Creating deployment (name:{}, schedulerName:{}) with masterUrl {} at {}",
                    deployment.getMetadata().getName(), deployment.getSpec().getTemplate().getSpec().getSchedulerName(),
                    fabricClient.getConfiguration().getMasterUrl(), System.currentTimeMillis());
            fabricClient.apps().deployments().inNamespace(namespace)
                    .create(this.deployment);
        }
    }

    private class EndDeployment implements Runnable {
        Deployment deployment;

        EndDeployment(final Deployment dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            LOG.info("Terminating deployment (name:{}, schedulerName:{}) with masterUrl {} at {}",
                    deployment.getMetadata().getName(), deployment.getSpec().getTemplate().getSpec().getSchedulerName(),
                    fabricClient.getConfiguration().getMasterUrl(), System.currentTimeMillis());
            fabricClient.resource(deployment).inNamespace(namespace).withGracePeriod(0).delete();
        }
    }
}