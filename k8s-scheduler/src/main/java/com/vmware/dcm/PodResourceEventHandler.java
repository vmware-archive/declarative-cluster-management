/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


/**
 * Subscribes to Kubernetes pod events and streams them to a flowable. Notably, it does not write
 * it to the database unlike the NodeResourceEventHandler. We do this to have tigher control over
 * batching these writes to the database.
 */
class PodResourceEventHandler implements ResourceEventHandler<Pod> {
    private static final Logger LOG = LoggerFactory.getLogger(PodResourceEventHandler.class);
    private final Consumer<PodEvent> podEventNotification;
    private final ExecutorService service;

    PodResourceEventHandler(final Consumer<PodEvent> podEventNotification) {
        this.podEventNotification = podEventNotification;
        this.service = Executors.newFixedThreadPool(10);
    }

    PodResourceEventHandler(final Consumer<PodEvent> podEventNotification, final ExecutorService service) {
        this.podEventNotification = podEventNotification;
        this.service = service;
    }


    public void onAddSync(final Pod pod) {
        LOG.trace("{} pod add received", pod.getMetadata().getName());
        podEventNotification.accept(new PodEvent(PodEvent.Action.ADDED, pod)); // might be better to add pods in a batch

    }

    public void onUpdateSync(final Pod oldPod, final Pod newPod) {
        final String oldPodScheduler = oldPod.getSpec().getSchedulerName();
        final String newPodScheduler = oldPod.getSpec().getSchedulerName();
        assert oldPodScheduler.equals(newPodScheduler);
        LOG.trace("{} => {} pod update received", oldPod.getMetadata().getName(), newPod.getMetadata().getName());
        podEventNotification.accept(new PodEvent(PodEvent.Action.UPDATED, newPod));
    }

    public void onDeleteSync(final Pod pod, final boolean deletedFinalStateUnknown) {
        final long now = System.nanoTime();
        LOG.trace("{} pod deleted ({}) in {}ns!", pod.getMetadata().getName(), deletedFinalStateUnknown,
                                                  (System.nanoTime() - now));
        podEventNotification.accept(new PodEvent(PodEvent.Action.DELETED, pod));
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