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
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class DataPuller {
    private static final Logger LOG = LoggerFactory.getLogger(DataPuller.class);

    void run(final DSLContext conn, final String url) {
        final ApiClient client = Config.fromUrl(url);
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
        Configuration.setDefaultApiClient(client);
        final SharedInformerFactory factory = new SharedInformerFactory();
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
        podInformer.addEventHandler(new PodResourceEventHandler(conn));

        LOG.info("Instantiated node and pod informers. Starting them all now.");
        factory.startAllRegisteredInformers();
    }
}