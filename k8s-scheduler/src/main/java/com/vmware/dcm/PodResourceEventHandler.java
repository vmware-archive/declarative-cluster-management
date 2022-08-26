/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Subscribes to Kubernetes pod events and streams them to a flowable. Notably, it does not write
 * it to the database unlike the NodeResourceEventHandler. We do this to have tighter control over
 * batching these writes to the database.
 */

record PodEvent(Action action, Pod pod, @Nullable Pod oldPod) {

    PodEvent(final Action action, final Pod pod) {
        this(action, pod, null);
    }

    enum Action {
        ADDED,
        UPDATED,
        DELETED
    }
}

class PodResourceEventHandler implements ResourceEventHandler<Pod> {
    private static final Logger LOG = LoggerFactory.getLogger(PodResourceEventHandler.class);
    private final Consumer<PodEvent> podEventNotification;
    private final ListeningExecutorService service;

    PodResourceEventHandler(final Consumer<PodEvent> podEventNotification) {
        this.podEventNotification = podEventNotification;
        this.service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(100));
    }

    PodResourceEventHandler(final Consumer<PodEvent> podEventNotification, final ExecutorService service) {
        this.podEventNotification = podEventNotification;
        this.service = MoreExecutors.listeningDecorator(service);
    }

    void shutdown() throws InterruptedException {
        service.shutdownNow();
        service.awaitTermination(100, TimeUnit.SECONDS);
    }

    public void onAddSync(final Pod pod) {
        LOG.trace("{} (uid: {}) pod add received", pod.getMetadata().getName(), pod.getMetadata().getUid());
        podEventNotification.accept(new PodEvent(PodEvent.Action.ADDED, pod)); // might be better to add pods in a batch

    }

    public void onUpdateSync(final Pod oldPod, final Pod newPod) {
        final String oldPodScheduler = oldPod.getSpec().getSchedulerName();
        final String newPodScheduler = newPod.getSpec().getSchedulerName();
        assert oldPodScheduler.equals(newPodScheduler);
        LOG.trace("{} => {} (uid: {}) pod update received", oldPod.getMetadata().getName(),
                  newPod.getMetadata().getName(), newPod.getMetadata().getUid());
        podEventNotification.accept(new PodEvent(PodEvent.Action.UPDATED, newPod, oldPod));
    }

    public void onDeleteSync(final Pod pod, final boolean deletedFinalStateUnknown) {
        final long now = System.nanoTime();
        LOG.trace("{} (uid: {}) pod deleted ({}) in {}ns!", pod.getMetadata().getName(), pod.getMetadata().getUid(),
                  deletedFinalStateUnknown, (System.nanoTime() - now));
        podEventNotification.accept(new PodEvent(PodEvent.Action.DELETED, pod));
    }


    public ListenableFuture<?> onAddAsync(final Pod pod) {
        return service.submit(() -> onAddSync(pod));
    }

    public ListenableFuture<?> onUpdateAsync(final Pod oldPod, final Pod newPod) {
        return service.submit(() -> onUpdateSync(oldPod, newPod));
    }

    public ListenableFuture<?> onDeleteAsync(final Pod pod, final boolean deletedFinalStateUnknown) {
        return service.submit(() -> onDeleteSync(pod, deletedFinalStateUnknown));
    }

    @Override
    public void onAdd(final Pod pod) {
        service.execute(() -> onAddSync(pod));
    }

    @Override
    public void onUpdate(final Pod oldPod, final Pod newPod) {
        service.execute(() -> onUpdateSync(oldPod, newPod));
    }

    @Override
    public void onDelete(final Pod pod, final boolean deletedFinalStateUnknown) {
        service.execute(() -> onDeleteSync(pod, deletedFinalStateUnknown));
    }
}