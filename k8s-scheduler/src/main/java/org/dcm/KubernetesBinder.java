/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.client.KubernetesClient;


/**
 * Pod -> node binding implementation that works with a real Kubernetes cluster
 */
class KubernetesBinder implements IPodToNodeBinder {
    private final KubernetesClient client;

    KubernetesBinder(final KubernetesClient client) {
        this.client = client;
    }

    @Override
    public void bindOne(final String namespace, final String podName, final String nodeName) {
        final Binding binding = new Binding();
        final ObjectReference target = new ObjectReference();
        final ObjectMeta meta = new ObjectMeta();
        target.setKind("Node");
        target.setApiVersion("v1");
        target.setName(nodeName);
        meta.setName(podName);
        binding.setTarget(target);
        binding.setMetadata(meta);
        client.bindings().inNamespace(namespace).create(binding);
    }
}
