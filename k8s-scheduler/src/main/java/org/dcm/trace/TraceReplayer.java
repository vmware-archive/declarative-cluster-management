/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.trace;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.dcm.IPodDeployer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TraceReplayer {
    private static final Logger LOG = LoggerFactory.getLogger(TraceReplayer.class);
    private final List<ListenableFuture<?>> deletions = new ArrayList<>();
    private final ListeningScheduledExecutorService scheduledExecutorService =
            MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(100));

    public void runTrace(final DefaultKubernetesClient client, final String fileName, final IPodDeployer deployer,
                  final String schedulerName, final int cpuScaleDown, final int memScaleDown,
                  final int timeScaleDown, final int startTimeCutOff) throws Exception {
        runTrace(client, fileName, deployer, schedulerName, cpuScaleDown, memScaleDown, timeScaleDown,
                startTimeCutOff, 0);
    }

    public void runTrace(final NamespacedKubernetesClient client, final String fileName, final IPodDeployer deployer,
                         final String schedulerName, final int cpuScaleDown, final int memScaleDown,
                         final int timeScaleDown, final int startTimeCutOff, final int affinityProportion)
                                                                                                throws Exception {
        runTrace(client, fileName, deployer, schedulerName, cpuScaleDown, memScaleDown, timeScaleDown,
                startTimeCutOff, 0, 60);
    }

    public void runTrace(final NamespacedKubernetesClient client, final String fileName, final IPodDeployer deployer,
                         final String schedulerName, final int cpuScaleDown, final int memScaleDown,
                         final int timeScaleDown, final int startTimeCutOff, final int affinityProportion,
                         final int deletionTime)
            throws Exception {
        LOG.info("Running trace with parameters: SchedulerName:{} CpuScaleDown:{}" +
                        " MemScaleDown:{} TimeScaleDown:{} StartTimeCutOff:{} AffinityProportion:{}",
                schedulerName, cpuScaleDown, memScaleDown, timeScaleDown, startTimeCutOff, affinityProportion);

        // Load data from file
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final InputStream inStream = classLoader.getResourceAsStream(fileName);
        Preconditions.checkNotNull(inStream);

        long maxStart = 0;
        long maxEnd = 0;
        int totalPods = 0;

        final int numberOfDeployments = getNumberOfDeployments(fileName); // returns the number of lines in the file

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inStream,
                StandardCharsets.UTF_8))) {
            String line;
            int taskCount = 0;
            final long startTime = System.currentTimeMillis();
            final Random r = new Random(1L); // providing deterministic seed
            System.out.println("Starting at " + startTime + " with " + numberOfDeployments + " deployments");

            while ((line = reader.readLine()) != null) {
                final String[] parts = line.split(" ", 7);
                final int start = Integer.parseInt(parts[2]) / timeScaleDown;
                // final int end = Integer.parseInt(parts[3]) / timeScaleDown;
                final float cpu = Float.parseFloat(parts[4].replace(">", "")) / cpuScaleDown;
                final float mem = Float.parseFloat(parts[5].replace(">", "")) / memScaleDown;
                final int vmCount = Integer.parseInt(parts[6].replace(">", ""));

                // If the deployment is not too large, then add affinity requirements according to probability
                final boolean createAffinityRequirements = vmCount < 400 &&
                        (r.nextInt(100) < affinityProportion);

                // generate a deployment's details based on cpu, mem requirements
                final List<Pod> deployment = getDeployment(client, schedulerName, cpu, mem, vmCount,
                        taskCount, createAffinityRequirements);
                totalPods += deployment.size();

                if (Integer.parseInt(parts[2]) > startTimeCutOff) { // window in seconds
                    break;
                }
                // get task time info
                final long taskStartTime = (long) start * 1000; // converting to millisec
                final long currentTime = System.currentTimeMillis();
                final long timeDiff = currentTime - startTime;
                final long waitTime = taskStartTime - timeDiff;

                // create deployment in the k8s cluster at the correct start time
                final ListenableFuture<?> scheduledStart = scheduledExecutorService.schedule(
                        deployer.startDeployment(deployment), waitTime, TimeUnit.MILLISECONDS);

                // get duration based on start and end times
                // final int duration = getDuration(start, end);

                final long computedEndTime = (waitTime / 1000) + deletionTime;

                // Schedule deletion of this deployment based on duration + time until start of the dep
                final SettableFuture<Boolean> onComplete = SettableFuture.create();
                scheduledStart.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                            scheduledExecutorService.schedule(deployer.endDeployment(deployment),
                                    deletionTime, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
                deletions.add(onComplete);

                maxStart = Math.max(maxStart, waitTime / 1000);
                maxEnd = Math.max(maxEnd, computedEndTime);

                // Add to a list to enable keeping the test active until deletion
                taskCount++;
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        LOG.info("All tasks launched ({} pods total). The latest application will start at {}s, and the last deletion" +
                        " will happen at {}s. Sleeping for {}s before teardown.", totalPods, maxStart,
                maxEnd, maxEnd);

        final List<Object> objects = Futures.successfulAsList(deletions).get(maxEnd + 100, TimeUnit.SECONDS);
        assert objects.size() != 0;
    }

    private int getNumberOfDeployments(final String fileName) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final InputStream inStream = classLoader.getResourceAsStream(fileName);
        int linesCount = 0;
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inStream,
                StandardCharsets.UTF_8))) {
            linesCount = (int) reader.lines().count();
        }  catch (final IOException exception) {
            throw new RuntimeException(exception);
        }
        return linesCount;
    }

    private List<Pod> getDeployment(final NamespacedKubernetesClient client, final String schedulerName, final float cpu,
                                    final float mem, final int count, final int taskCount,
                                    final boolean createAffinityRequirements) {
        // Load the template file and update its contents to generate a new deployment template
        final String podFile = createAffinityRequirements ? "pod-with-affinity.yml" : "pod-only.yml";
        final List<Pod> podsToCreate = IntStream.range(0, count)
                .mapToObj(podCount -> {
                    try (final InputStream fileStream =
                                 getClass().getClassLoader().getResourceAsStream(podFile)) {
                        final Pod pod = client.pods().load(fileStream).get();
                        pod.getSpec().setSchedulerName(schedulerName);
                        final String appName = String.format("app%s-%s", createAffinityRequirements ? "aff" : "",
                                taskCount);
                        pod.getMetadata().setName(appName + "-" + podCount);

                        if (createAffinityRequirements) {
                            // creating anti-affinity affinity requirements, with In operator
                            final Map<String, String> labels = pod.getMetadata().getLabels();
                            final Map<String, String> newLabels = new HashMap<>();
                            for (final Map.Entry<String, String> entry: labels.entrySet()) {
                                newLabels.put(entry.getKey() + taskCount, entry.getValue() + taskCount);
                            }
                            pod.getMetadata().setLabels(newLabels);

                            final List<PodAffinityTerm> podAffinityTerms = pod.getSpec().getAffinity()
                                    .getPodAntiAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
                            for (final PodAffinityTerm term: podAffinityTerms) {
                                final List<LabelSelectorRequirement> requirements =
                                        term.getLabelSelector().getMatchExpressions();
                                for (final LabelSelectorRequirement requirement: requirements) {
                                    requirement.setKey(requirement.getKey() + taskCount);
                                    final List<String> newValues = new ArrayList<>();
                                    requirement.getValues().forEach(v -> newValues.add(v + taskCount));
                                    requirement.setValues(newValues);
                                }
                            }
                        }

                        final List<Container> containerList = pod.getSpec().getContainers();
                        for (ListIterator<Container> iter = containerList.listIterator(); iter.hasNext(); ) {
                            final Container container = iter.next();
                            final ResourceRequirements resReq = new ResourceRequirements();
                            final Map<String, Quantity> reqs = new HashMap<>();
                            reqs.put("cpu", new Quantity(cpu * 1000 + "m"));
                            reqs.put("memory", new Quantity(Float.toString(mem)));
                            resReq.setRequests(reqs);
                            container.setResources(resReq);
                            iter.set(container);
                        }
                        return pod;
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        return podsToCreate;
    }
}