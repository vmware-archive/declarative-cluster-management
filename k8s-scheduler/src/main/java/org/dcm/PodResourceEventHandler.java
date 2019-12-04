/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.reactivex.processors.PublishProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PodResourceEventHandler implements ResourceEventHandler<Pod> {
    private static final Logger LOG = LoggerFactory.getLogger(PodResourceEventHandler.class);
    private final PublishProcessor<PodEvent> flowable;

    PodResourceEventHandler(final PublishProcessor<PodEvent> flowable) {
        this.flowable = flowable;
    }

    @Override
    public void onAdd(final Pod pod) {
        LOG.info("{} pod add received", pod.getMetadata().getName());
        flowable.onNext(new PodEvent(PodEvent.Action.ADDED, pod)); // might be better to add pods in a batch
    }

    @Override
    public void onUpdate(final Pod oldPod, final Pod newPod) {
        final String oldPodScheduler = oldPod.getSpec().getSchedulerName();
        final String newPodScheduler = oldPod.getSpec().getSchedulerName();
        assert oldPodScheduler.equals(newPodScheduler);
        LOG.debug("{} => {} pod update received", oldPod.getMetadata().getName(), newPod.getMetadata().getName());
        flowable.onNext(new PodEvent(PodEvent.Action.UPDATED, newPod));
    }

    @Override
    public void onDelete(final Pod pod, final boolean deletedFinalStateUnknown) {
        final long now = System.nanoTime();
        LOG.debug("{} pod deleted in {}ns!", pod.getMetadata().getName(), (System.nanoTime() - now));
        flowable.onNext(new PodEvent(PodEvent.Action.DELETED, pod));
    }
}