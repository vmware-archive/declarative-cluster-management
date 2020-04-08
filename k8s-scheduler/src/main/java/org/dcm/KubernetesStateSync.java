/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */


package org.dcm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

class KubernetesStateSync {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateSync.class);
    private final SharedInformerFactory sharedInformerFactory;
    private final ThreadFactory namedThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("flowable-thread-%d").build();
    private final ExecutorService service = Executors.newFixedThreadPool(10, namedThreadFactory);

    KubernetesStateSync(final KubernetesClient client) {
        this.sharedInformerFactory = client.informers();
    }

    Flowable<PodEvent> setupInformersAndPodEventStream(final DBConnectionPool dbConnectionPool) {

        final SharedIndexInformer<Node> nodeSharedIndexInformer = sharedInformerFactory
                .sharedIndexInformerFor(Node.class, NodeList.class, 30000);
        nodeSharedIndexInformer.addEventHandler(new NodeResourceEventHandler(dbConnectionPool, service));

        // Pod informer
        final SharedIndexInformer<Pod> podInformer = sharedInformerFactory
                .sharedIndexInformerFor(Pod.class, PodList.class, 30000);
        final PublishProcessor<PodEvent> podEventPublishProcessor = PublishProcessor.create();
        podInformer.addEventHandler(new PodResourceEventHandler(podEventPublishProcessor, service));

        LOG.info("Instantiated node and pod informers. Starting them all now.");

        return podEventPublishProcessor;
    }

    void startProcessingEvents() {
        sharedInformerFactory.startAllRegisteredInformers();
    }

    void shutdown() {
        sharedInformerFactory.stopAllRegisteredInformers();
    }
}