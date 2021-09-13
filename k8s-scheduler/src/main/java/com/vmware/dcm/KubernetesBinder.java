/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.jooq.Record;

import java.util.List;
import java.util.UUID;
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
        notifySuccess(namespace, podName, nodeName);
    }

    @Override
    public void bindManyAsnc(final List<? extends Record> records) {
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

    @Override
    public void unbindManyAsnc(final List<? extends Record> records) {
        ForkJoinPool.commonPool().execute(() -> records.forEach(
                r -> service.execute(() -> {
                        final String podName = r.get("POD_NAME", String.class);
                        final String namespace = r.get("NAMESPACE", String.class);
                        client.pods().inNamespace(namespace).withName(podName).delete();
                    }
                )
        ));
    }

    public void notifySuccess(final String namespace, final String podName, final String nodeName) {
        final ObjectReference podRef = new ObjectReference();
        podRef.setKind("Pod");
        podRef.setApiVersion("v1");
        podRef.setName(podName);
        podRef.setNamespace(namespace);
        final Event event = new Event();
        final ObjectMeta meta = new ObjectMeta();
        meta.setName(podName + UUID.randomUUID());
        event.setMetadata(meta);
        event.setApiVersion("v1");
        event.setMessage(String.format("Successfully assigned %s/%s to %s", namespace, podName, nodeName));
        event.setInvolvedObject(podRef);
        event.setReason("Scheduled");
        event.setType("Normal");
        try {
            client.v1().events().inNamespace(namespace).create(event);
        } catch (final KubernetesClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void notifyFail(final Record record) {
        final String podName = record.get("POD_NAME", String.class);
        final String namespace = record.get("NAMESPACE", String.class);
        final ObjectReference podRef = new ObjectReference();
        podRef.setKind("Pod");
        podRef.setApiVersion("v1");
        podRef.setName(podName);
        podRef.setNamespace(namespace);
        final Event event = new Event();
        final ObjectMeta meta = new ObjectMeta();
        meta.setName(podName + UUID.randomUUID());
        event.setMetadata(meta);
        event.setApiVersion("v1");
        event.setMessage("looooooooooooooooool");
        event.setInvolvedObject(podRef);
        event.setReason("FailedScheduling");
        event.setType("Warning");
        try {
            client.v1().events().inNamespace(namespace).create(event);
        } catch (final KubernetesClientException e) {
            e.printStackTrace();
        }
    }
}
