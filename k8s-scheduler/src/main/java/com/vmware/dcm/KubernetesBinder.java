/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.jooq.Record;
import org.jooq.Result;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;


/**
 * Pod -> node binding implementation that works with a real Kubernetes cluster
 */
class KubernetesBinder implements IPodToNodeBinder {
    private final KubernetesClient client;
    private final ThreadFactory namedThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("bind-thread-%d").build();
    private final ExecutorService service = Executors.newFixedThreadPool(10, namedThreadFactory);

    KubernetesBinder(final KubernetesClient client) {
        this.client = client;
    }

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

    @Override
    public void bindManyAsnc(final Result<? extends Record> records) {
        ForkJoinPool.commonPool().execute(() -> records.forEach(
                r -> service.execute(() -> {
                    final String podName = r.get("POD_NAME", String.class);
                    final String namespace = r.get("NAMESPACE", String.class);
                    final String nodeName = r.get("CONTROLLABLE__NODE_NAME", String.class);
                    bindOne(namespace, podName, nodeName);
                }
            )
        ));
    }
}
