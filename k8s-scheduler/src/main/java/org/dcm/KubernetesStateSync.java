/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */


package org.dcm;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeList;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.Config;
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
    final SharedInformerFactory factory = new SharedInformerFactory();

    Flowable<List<PodEvent>> setupInformersAndPodEventStream(final DSLContext conn, final String url,
                                                             final int batchCount, final long batchTimeMs) {
        updateBatchCount(conn, batchCount);
        final ApiClient client = Config.fromUrl(url);
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
        Configuration.setDefaultApiClient(client);
        final CoreV1Api coreV1Api = new CoreV1Api();

        // Node informer
       final SharedIndexInformer<V1Node> nodeInformer = factory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> {
                    try {
                        return coreV1Api.listNodeCall(null, null, null, null, null, null,
                                params.resourceVersion, params.timeoutSeconds, params.watch, null, null);
                    } catch (final ApiException e) {
                        throw new RuntimeException(e);
                    }
                }, V1Node.class, V1NodeList.class);
        nodeInformer.addEventHandler(new NodeResourceEventHandler(conn));

        // Pod informer
        final SharedIndexInformer<V1Pod> podInformer = factory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> {
                    try {
                        return coreV1Api.listPodForAllNamespacesCall(null, null, null, null, null,
                                null, params.resourceVersion, params.timeoutSeconds, params.watch, null, null);
                    } catch (final ApiException e) {
                        throw new RuntimeException(e);
                    }
                }, V1Pod.class, V1PodList.class);
        final PublishProcessor<PodEvent> podEventPublishProcessor = PublishProcessor.create();
        podInformer.addEventHandler(new PodResourceEventHandler(conn, podEventPublishProcessor));

        LOG.info("Instantiated node and pod informers. Starting them all now.");

        return podEventPublishProcessor.buffer(batchTimeMs, TimeUnit.MILLISECONDS, batchCount)
                       .filter(podEvents -> !podEvents.isEmpty());
    }

    void startProcessingEvents() {
        factory.startAllRegisteredInformers();
    }

    void updateBatchCount(final DSLContext conn, final int batchCount) {
        conn.insertInto(Tables.BATCH_SIZE).values(batchCount).execute();
    }
}