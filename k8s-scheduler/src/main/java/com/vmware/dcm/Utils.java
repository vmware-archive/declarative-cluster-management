/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

class Utils {

    static long convertUnit(final Quantity quantity, final String resourceName) {
        if (resourceName.equals("cpu")) {
            final String unit = quantity.getFormat();
            if (unit.equals("")) {
                // Convert to milli-cpu
                return (long) (Double.parseDouble(quantity.getAmount()) * 1000);
            } else if (unit.equals("m")) {
                return (long) (Double.parseDouble(quantity.getAmount()));
            }
            throw new IllegalArgumentException(quantity + " for resource type: " + resourceName);
        } else {
            // Values are guaranteed to be under 2^63 - 1, so this is safe
            return Quantity.getAmountInBytes(quantity).longValue();
        }
    }

    static void compileDDlog(@Nullable final String ddlogFile) {
        final var ddlogConn = new DDlogDBConnectionPool(ddlogFile);
        final var autoScopeViews = Scheduler.autoScopeViews(20);
        ddlogConn.addScopedViews(autoScopeViews.extraViews());
        ddlogConn.buildDDlog(true, true);
    }

    /*
     * XXX: The below methods are only used by NodeResourceEventHandler as a temporary workaround. Remove
     *      when the workaround is no longer needed.
     */
    static Pod newPod(final String name) {
        return newPod(name, "Pending");
    }

    static Pod newPod(final String name, final String status) {
        return newPod(name, UUID.randomUUID(), status, Collections.emptyMap(), Collections.emptyMap());
    }

    static Pod newPod(final String podName, final UUID uid, final String phase,
                      final Map<String, String> selectorLabels, final Map<String, String> labels) {
        final Pod pod = new Pod();
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp("1");
        meta.setNamespace("default");
        meta.setResourceVersion("0");
        final PodSpec spec = new PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        spec.setNodeSelector(selectorLabels);

        final Container container = new Container();
        container.setName("ignore");
        container.setImage("ignore");

        final ResourceRequirements resourceRequirements = new ResourceRequirements();
        resourceRequirements.setRequests(Collections.emptyMap());
        container.setResources(resourceRequirements);
        spec.getContainers().add(container);

        final Affinity affinity = new Affinity();
        final NodeAffinity nodeAffinity = new NodeAffinity();
        affinity.setNodeAffinity(nodeAffinity);
        spec.setAffinity(affinity);
        final PodStatus status = new PodStatus();
        status.setPhase(phase);
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }
}