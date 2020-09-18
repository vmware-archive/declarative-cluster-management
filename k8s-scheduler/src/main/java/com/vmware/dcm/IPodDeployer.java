/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Pod;

import java.util.List;

/**
 * An interface used by WorkloadGeneratorIT.runTrace() to create and delete pod deployments. It allows
 * us to replay traces locally or against an actual Kubernetes cluster
 */
public interface IPodDeployer {

    Runnable startDeployment(final List<Pod> deployment);

    Runnable endDeployment(final List<Pod> deployment);
}
