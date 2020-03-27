/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.apps.Deployment;

/**
 * An interface used by WorkloadGeneratorIT.runTrace() to create and delete pod deployments. It allows
 * us to replay traces locally or against an actual Kubernetes cluster
 */
public interface IPodDeployer {

    Runnable startDeployment(final Deployment deployment);

    Runnable endDeployment(final Deployment deployment);
}
