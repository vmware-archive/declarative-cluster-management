/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * To run these specific tests, pass a `schedulerName` property to maven, for example:
 *
 *  mvn integrate-test -DargLine="-Dk8sUrl=<hostname>:<port> -DschedulerName=dcm-scheduler"
 */
public class WorkloadGeneratorIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(WorkloadGeneratorIT.class);
    private static final String SCHEDULER_NAME_PROPERTY = "schedulerName";
    private static final String SCHEDULER_NAME_DEFAULT = "default-scheduler";
    @Nullable private static String schedulerName;

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(100);
    private final List<ScheduledFuture> startDepList = new ArrayList<>();
    private final List<ScheduledFuture> endDepList = new ArrayList<>();

    @BeforeAll
    public static void setSchedulerFromEnvironment() {
        final String property = System.getProperty(SCHEDULER_NAME_PROPERTY);
        schedulerName = property == null ? SCHEDULER_NAME_DEFAULT : property;
    }

    @BeforeEach
    public void logBuildInfo() {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                Charset.forName("UTF8")))) {
            final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
            LOG.info("Running integration test for the following build: {}", gitProperties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSmallTrace() throws Exception {
        if (getClass().getClassLoader().getResource("test-data.txt") != null) {
            System.out.println("Running small trace");
            runTrace("test-data.txt");
        } else {
            System.out.println("test file not found");
        }
    }

    @Test
    public void testAzureV1Complete() throws Exception {
        if (getClass().getClassLoader().getResource("v1-data.txt") != null) {
            System.out.println("Running Azure v1 complete trace");
            runTrace("v1-data.txt");
        } else {
            System.out.println("Azure v1 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Test
    public void testAzureV1Cropped() throws Exception {
        if (getClass().getClassLoader().getResource("v1-cropped.txt") != null) {
            System.out.println("Running Azure v1 cropped trace");
            runTrace("v1-cropped.txt");
        } else {
            System.out.println("Azure v1 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Test
    public void testAzureV2Complete() throws Exception {
        if (getClass().getClassLoader().getResource("v2-data.txt") != null) {
            System.out.println("Running Azure v2 complete trace");
            runTrace("v2-data.txt");
        } else {
            System.out.println("Azure v2 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Test
    public void testAzureV2Cropped() throws Exception {
        if (getClass().getClassLoader().getResource("v2-cropped.txt") != null) {
            System.out.println("Running Azure v2 cropped trace");
            runTrace("v2-cropped.txt");
        } else {
            System.out.println("Azure v2 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    public void runTrace(final String fileName) throws Exception {
        // Trace pod and node arrivals/departure
        final long traceId = System.currentTimeMillis();
        fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
        fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

        assertNotNull(schedulerName);
        LOG.info("Running testAffinityAntiAffinity with parameters: MasterUrl:{} SchedulerName:{}",
                fabricClient.getConfiguration().getMasterUrl(), schedulerName);

        // Load data from file
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final InputStream inStream = classLoader.getResourceAsStream(fileName);

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inStream,
                Charset.forName("UTF8")))) {
            String line;
            int taskCount = 0;
            final long startTime = System.currentTimeMillis();
            System.out.println("Starting at " + startTime);
            while ((line = reader.readLine()) != null) {
                final String[] parts = line.split(" ", 7);
                final Integer start = Integer.parseInt(parts[2]);
                final Integer end = Integer.parseInt(parts[3]);
                final Float cpu = Float.parseFloat(parts[4]);
                final Float mem = Float.parseFloat(parts[5]);
                final Integer vmCount = Integer.parseInt(parts[6]);

                // generate a deployment's details based on cpu, mem requirements
                final Deployment deployment = getDeployment(cpu, mem, vmCount, taskCount);

                // get task time info
                final long taskStartTime = (long) start * 1000; // converting to millisec
                final long currentTime = System.currentTimeMillis();
                final long timeDiff = currentTime - startTime;
                final long waitTime = taskStartTime - timeDiff;

                // create deployment in the k8s cluster at the correct start time
                final ScheduledFuture scheduledStart = scheduledExecutorService.schedule(
                        new StartDeployment(deployment), waitTime, TimeUnit.MILLISECONDS);

                // get duration based on start and end times
                final int duration = getDuration(start, end);

                // Schedule deletion of this deployment based on duration + time until start of the dep
                final ScheduledFuture scheduledEnd = scheduledExecutorService.schedule(
                        new EndDeployment(deployment), (waitTime / 1000) + duration, TimeUnit.SECONDS);

                // Add to a list to enable keeping the test active until deletion
                startDepList.add(scheduledStart);
                endDepList.add(scheduledEnd);

                taskCount++;
            }

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Wait until all scheduled deletes are completed
        for (final ScheduledFuture end: endDepList) {
            end.get();
        }
    }

    private Deployment getDeployment(final float cpu, final float mem, final int count, final int taskCount) {
        final URL url = getClass().getClassLoader().getResource("app-no-constraints.yml");
        assertNotNull(url);
        final File file = new File(url.getFile());

        // Load the template file and update its contents to generate a new deployment template
        final Deployment deployment = fabricClient.apps().deployments().load(file).get();
        deployment.getSpec().getTemplate().getSpec().setSchedulerName(schedulerName);
        final String appName = "app-" + taskCount;
        deployment.getMetadata().setName(appName);
        deployment.getSpec().setReplicas(count);

        final List<Container> containerList = deployment.getSpec().getTemplate().getSpec().getContainers();
        for (ListIterator<Container> iter = containerList.listIterator(); iter.hasNext(); ) {
            final Container container = iter.next();
            final ResourceRequirements resReq = new ResourceRequirements();
            final Map<String, Quantity> reqs = new HashMap<String, Quantity>();
            reqs.put("cpu", new Quantity(cpu * 1000 + "m"));
            reqs.put("memory", new Quantity(Float.toString(mem)));
            resReq.setRequests(reqs);
            container.setResources(resReq);
            iter.set(container);
        }
        deployment.getSpec().getTemplate().getSpec().setContainers(containerList);
        return deployment;
    }

    private int getDuration(final int startTime, int endTime) {
        if (endTime <= startTime) {
            endTime = startTime + 5;
        }
        final int duration = (endTime - startTime);
        return duration;
    }

    private static class StartDeployment implements Runnable {
        Deployment deployment;

        StartDeployment(final Deployment dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            System.out.println("Creating deployment " + deployment.getMetadata().getName() +
                    " at " + System.currentTimeMillis());
            fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE)
                    .create(deployment);
        }
    }

    private static class EndDeployment implements Runnable {
        Deployment deployment;

        EndDeployment(final Deployment dep) {
            this.deployment = dep;
        }

        @Override
        public void run() {
            System.out.println("Terminating deployment " + deployment.getMetadata().getName() +
                    " at " + System.currentTimeMillis());
            fabricClient.apps().deployments().inNamespace(TEST_NAMESPACE)
                    .delete(deployment);
        }
    }


    private static final class LoggingPodWatcher implements Watcher<Pod> {
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
        public void onClose(final KubernetesClientException cause) {
            LOG.info("Timestamp: {}, Trace: {}, PodWatcher closed", System.currentTimeMillis(), traceId);
        }
    }


    private static final class LoggingNodeWatcher implements Watcher<Node> {
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
        public void onClose(final KubernetesClientException cause) {
            LOG.info("Timestamp: {}, Trace: {}, NodeWatcher closed", System.currentTimeMillis(), traceId);
        }
    }
}