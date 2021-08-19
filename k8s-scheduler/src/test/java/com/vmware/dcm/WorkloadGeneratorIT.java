/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.trace.TraceReplayer;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * To run these specific tests, pass a `schedulerName` property to maven, for example:
 *
 *  mvn integrate-test -DargLine="-Dk8sUrl=<hostname>:<port> -DschedulerName=dcm-scheduler"
 */
class WorkloadGeneratorIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(WorkloadGeneratorIT.class);
    private static final String SCHEDULER_NAME_PROPERTY = "schedulerName";
    private static final String SCHEDULER_NAME_DEFAULT = "default-scheduler";
    private static final String CPU_SCALE_DOWN_PROPERTY = "cpuScaleDown";
    private static final int CPU_SCALE_DOWN_DEFAULT = 40;
    private static final String MEM_SCALE_DOWN_PROPERTY = "memScaleDown";
    private static final int MEM_SCALE_DOWN_DEFAULT = 50;
    private static final String TIME_SCALE_DOWN_PROPERTY = "timeScaleDown";
    private static final int TIME_SCALE_DOWN_DEFAULT = 1000;
    private static final String START_TIME_CUTOFF = "startTimeCutOff";
    private static final int START_TIME_CUTOFF_DEFAULT = 1000;
    private static final String AFFINITY_PROPORTION = "affinityProportion";
    private static final int AFFINITY_PROPORTION_DEFAULT = 0;

    @BeforeEach
    public void logBuildInfo() {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
            LOG.info("Running integration test for the following build: {}", gitProperties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Tag("experiment")
    @Test
    public void testAzureV1Complete() throws Exception {
        if (getClass().getClassLoader().getResource("v1-data.txt") != null) {
            System.out.println("Running Azure v1 complete trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v1-data.txt", deployer);
        } else {
            System.out.println("Azure v1 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Tag("experiment")
    @Test
    public void testAzureV1Cropped() throws Exception {
        if (getClass().getClassLoader().getResource("v1-cropped.txt") != null) {
            System.out.println("Running Azure v1 cropped trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v1-cropped.txt", deployer);
        } else {
            System.out.println("Azure v1 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Tag("experiment")
    @Test
    public void testAzureV2Complete() throws Exception {
        if (getClass().getClassLoader().getResource("v2-data.txt") != null) {
            System.out.println("Running Azure v2 complete trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v2-data.txt", deployer);
        } else {
            System.out.println("Azure v2 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Tag("experiment")
    @Test
    public void testAzureV2Cropped() throws Exception {
        if (getClass().getClassLoader().getResource("v2-cropped.txt") != null) {
            System.out.println("Running Azure v2 cropped trace");
            // Trace pod and node arrivals/departure
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v2-cropped.txt", deployer);
        } else {
            System.out.println("Azure v2 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    private void runTrace(final String fileName, final IPodDeployer deployer) throws Exception {
        final String schedulerNameProperty = System.getProperty(SCHEDULER_NAME_PROPERTY);
        final String schedulerName = schedulerNameProperty == null ? SCHEDULER_NAME_DEFAULT : schedulerNameProperty;

        final String cpuScaleProperty = System.getProperty(CPU_SCALE_DOWN_PROPERTY);
        final int cpuScaleDown = cpuScaleProperty == null ? CPU_SCALE_DOWN_DEFAULT : Integer.parseInt(cpuScaleProperty);

        final String memScaleProperty = System.getProperty(MEM_SCALE_DOWN_PROPERTY);
        final int memScaleDown = memScaleProperty == null ? MEM_SCALE_DOWN_DEFAULT : Integer.parseInt(memScaleProperty);

        final String timeScaleProperty = System.getProperty(TIME_SCALE_DOWN_PROPERTY);
        final int timeScaleDown = timeScaleProperty == null ? TIME_SCALE_DOWN_DEFAULT :
                                        Integer.parseInt(timeScaleProperty);

        final String startTimeCutOffProperty = System.getProperty(START_TIME_CUTOFF);
        final int startTimeCutOff = startTimeCutOffProperty == null ? START_TIME_CUTOFF_DEFAULT :
                Integer.parseInt(startTimeCutOffProperty);

        final String affinityProportionProperty = System.getProperty(AFFINITY_PROPORTION);
        final int affinityProportion = affinityProportionProperty == null ? AFFINITY_PROPORTION_DEFAULT :
                Integer.parseInt(affinityProportionProperty);
        final TraceReplayer traceReplayer = new TraceReplayer();
        traceReplayer.runTrace(fabricClient, fileName, deployer, schedulerName, cpuScaleDown, memScaleDown,
                               timeScaleDown, startTimeCutOff, affinityProportion);
    }

    static final class LoggingPodWatcher implements Watcher<Pod> {
        private final long traceId;

        LoggingPodWatcher(final long traceId) {
            this.traceId = traceId;
        }

        @Override
        public void eventReceived(final Action action, final Pod pod) {
            LOG.info("Timestamp: {}, Trace: {}, PodName: {}, NodeName: {}, Status: {}, Action: {}",
                    System.currentTimeMillis(), traceId, pod.getMetadata().getName(), pod.getSpec().getNodeName(),
                    pod.getStatus().getPhase(), action);
        }

        @Override
        public void onClose(final WatcherException cause) {
            LOG.info("Timestamp: {}, Trace: {}, PodWatcher closed", System.currentTimeMillis(), traceId);
        }
    }


    static final class LoggingNodeWatcher implements Watcher<Node> {
        private final long traceId;

        LoggingNodeWatcher(final long traceId) {
            this.traceId = traceId;
        }

        @Override
        public void eventReceived(final Action action, final Node node) {
            LOG.info("Timestamp: {}, Trace: {}, NodeName: {}, Action: {}", System.currentTimeMillis(), traceId,
                    node.getMetadata().getName(), action);
        }

        @Override
        public void onClose(final WatcherException cause) {
            LOG.info("Timestamp: {}, Trace: {}, NodeWatcher closed", System.currentTimeMillis(), traceId);
        }
    }
}
