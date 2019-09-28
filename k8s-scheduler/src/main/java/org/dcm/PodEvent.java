package org.dcm;

import io.kubernetes.client.models.V1Pod;

class PodEvent {

    enum Action {
        ADDED,
        UPDATED,
        DELETED
    }

    private final Action action;
    private final V1Pod pod;

    PodEvent(final Action action, final V1Pod pod) {
        this.action = action;
        this.pod = pod;
    }

    public Action getAction() {
        return action;
    }

    public V1Pod getPod() {
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
