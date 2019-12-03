/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.apps.Deployment;

public interface IDeployer {

    Runnable startDeployment(final Deployment deployment);

    Runnable endDeployment(final Deployment deployment);
}
