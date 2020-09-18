/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Pod;

class PodEvent {

    enum Action {
        ADDED,
        UPDATED,
        DELETED
    }

    private final Action action;
    private final Pod pod;

    PodEvent(final Action action, final Pod pod) {
        this.action = action;
        this.pod = pod;
    }

    public Action getAction() {
        return action;
    }

    public Pod getPod() {
        return pod;
    }

    @Override
    public String toString() {
        return "PodEvent{" +
                "action=" + action.name() +
                ", pod=" + pod.getMetadata().getName() +
                '}';
    }
}
