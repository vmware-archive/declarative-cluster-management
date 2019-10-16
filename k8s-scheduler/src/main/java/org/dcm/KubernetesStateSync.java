/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */


package org.dcm;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

class KubernetesStateSync {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateSync.class);

    Flowable<List<PodEvent>> setupInformersAndPodEventStream(final DSLContext conn,
                                                             final DefaultKubernetesClient client,
                                                             final int batchCount, final long batchTimeMs) {
        updateBatchCount(conn, batchCount);
        final SharedIndexInformer<Node> nodeSharedIndexInformer = client.informers()
                .sharedIndexInformerFor(Node.class, NodeList.class, 30000);
        nodeSharedIndexInformer.addEventHandler(new NodeResourceEventHandler(conn));

        // Pod informer
        final SharedIndexInformer<Pod> podInformer = client.informers()
                .sharedIndexInformerFor(Pod.class, PodList.class, 30000);
        final PublishProcessor<PodEvent> podEventPublishProcessor = PublishProcessor.create();
        podInformer.addEventHandler(new PodResourceEventHandler(conn, podEventPublishProcessor));

        LOG.info("Instantiated node and pod informers. Starting them all now.");

        return podEventPublishProcessor
                       .filter(podEvent -> podEvent.getAction().equals(PodEvent.Action.ADDED)
                               && podEvent.getPod().getStatus().getPhase().equals("Pending")
                               && podEvent.getPod().getSpec().getNodeName() == null
                               && podEvent.getPod().getSpec().getSchedulerName().equals(
                                           Scheduler.SCHEDULER_NAME)
                                      )
                       .buffer(batchTimeMs, TimeUnit.MILLISECONDS, batchCount)
                       .filter(podEvents -> !podEvents.isEmpty());
    }

    void startProcessingEvents(final DefaultKubernetesClient client) {
        client.informers().startAllRegisteredInformers();
    }

    void updateBatchCount(final DSLContext conn, final int batchCount) {
        conn.insertInto(Tables.BATCH_SIZE).values(batchCount).execute();
    }

    void shutdown(final DefaultKubernetesClient client) {
        client.informers().stopAllRegisteredInformers();
    }
}