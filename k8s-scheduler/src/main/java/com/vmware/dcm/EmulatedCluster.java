/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vmware.dcm.trace.TraceReplayer;
import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.CalciteSqlStatement;
import com.vmware.ddlog.util.sql.CalciteToH2Translator;
import com.vmware.ddlog.util.sql.CalciteToPrestoTranslator;
import com.vmware.ddlog.util.sql.H2SqlStatement;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * Used to replay traces in-process and emulate large clusters without the involvement of an actual Kubernetes cluster.
 */
class EmulatedCluster {
    private static final Logger LOG = LoggerFactory.getLogger(EmulatedCluster.class);

    public static void compileAndLoad(final List<CalciteSqlStatement> ddl, final List<String> createIndexStatements)
            throws IOException, DDlogException {
        final Translator t = new Translator(null);
        CalciteToPrestoTranslator ctopTranslator = new CalciteToPrestoTranslator();
        ddl.forEach(x -> t.translateSqlStatement(ctopTranslator.toPresto(x)));
        createIndexStatements.forEach(t::translateCreateIndexStatement);

        final DDlogProgram dDlogProgram = t.getDDlogProgram();
        final String fileName = "/tmp/program.dl";
        File tmp = new File(fileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(dDlogProgram.toString());
        bw.close();
        DDlogAPI.CompilationResult result = new DDlogAPI.CompilationResult(true);
        final String ddlogHome = System.getenv("DDLOG_HOME");
        assertNotNull(ddlogHome);
        DDlogAPI.compileDDlogProgram(fileName, result, ddlogHome + "/lib", ddlogHome + "/sql/lib");
        if (!result.isSuccess())
            throw new RuntimeException("Failed to compile ddlog program");
        DDlogAPI.loadDDlog();
    }

    public static DDlogDBConnectionPool setupDDlog() {
        try {
            List<String> tables = DDlogDBViews.getSchema();
            CalciteToH2Translator translator = new CalciteToH2Translator();

            // The `create index` statements are for H2 and not for the DDlog backend
            List<String> createIndexStatements = new ArrayList<>();
            List<CalciteSqlStatement> tablesInCalcite = new ArrayList<>();

            tables.forEach(x -> {
                if (x.startsWith("create index")) {
                    createIndexStatements.add(x);
                } else {
                    tablesInCalcite.add(new CalciteSqlStatement((x)));
                }
            });

            compileAndLoad(tablesInCalcite, createIndexStatements);

            final DDlogAPI dDlogAPI = new DDlogAPI(1, false);

            // Initialise the data provider
            final DDlogJooqProvider provider = new DDlogJooqProvider(dDlogAPI,
                    Stream.concat(
                            tablesInCalcite.stream().map(translator::toH2),
                            createIndexStatements.stream().map(H2SqlStatement::new)).collect(Collectors.toList()));
            return new DDlogDBConnectionPool(provider);
        } catch (Exception e) {
            throw new RuntimeException("Could not set up DDlog backend: " + e.getMessage());
        }
    }

    public void runTraceLocally(final int numNodes, final String traceFileName, final int cpuScaleDown,
                                final int memScaleDown, final int timeScaleDown, final int startTimeCutOff,
                                final int affinityRequirementsProportion, final boolean scopeOn)
            throws Exception {
        final IConnectionPool dbConnectionPool = setupDDlog(); // new DBConnectionPool();

        final ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("flowable-thread-%d").build();
        final ExecutorService service = Executors.newFixedThreadPool(10, namedThreadFactory);

        // Add all nodes
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool,
                service);

        final int solverMaxTimeInSeconds = numNodes >= 5000 ? 30 : 1;
        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                                                 .setDebugMode(true)
                                                 .setNumThreads(4)
                                                 .setScopedInitialPlacement(scopeOn)
                                                 .setSolverMaxTimeInSeconds(solverMaxTimeInSeconds).build();
        final PodResourceEventHandler handler = new PodResourceEventHandler(scheduler::handlePodEvent, service);
        scheduler.startScheduler(new EmulatedPodToNodeBinder(dbConnectionPool));
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, UUID.randomUUID(), Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity("2"));
            node.getStatus().getCapacity().put("memory", new Quantity("2000"));
            node.getStatus().getCapacity().put("pods", new Quantity("110"));
            nodeResourceEventHandler.onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-" + nodeName;
            final String status = "Running";
            final Pod pod = newPod(podName, UUID.randomUUID(), status, Collections.emptyMap(), Collections.emptyMap());
            final Map<String, Quantity> resourceRequests = new HashMap<>();
            resourceRequests.put("cpu", new Quantity("100m"));
            resourceRequests.put("memory", new Quantity("1"));
            resourceRequests.put("pods", new Quantity("1"));
            pod.getMetadata().setNamespace("kube-system");
            pod.getMetadata().setResourceVersion("1");
            pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
            pod.getSpec().setNodeName(nodeName);
            handler.onAddSync(pod);
        }
        final TraceReplayer traceReplayer = new TraceReplayer();
        final IPodDeployer deployer = new EmulatedPodDeployer(handler, "default");
        final DefaultKubernetesClient client = new DefaultKubernetesClient();
        traceReplayer.runTrace(client, traceFileName, deployer, "dcm-scheduler", numNodes,
                cpuScaleDown, memScaleDown, timeScaleDown, startTimeCutOff, affinityRequirementsProportion, 5);
    }

    private static Node addNode(final String nodeName, final UUID uid, final Map<String, String> labels,
                                final List<NodeCondition> conditions) {
        final Node node = new Node();
        final NodeStatus status = new NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("10000"));
        quantityMap.put("memory", new Quantity("10000"));
        quantityMap.put("ephemeral-storage", new Quantity("10000"));
        quantityMap.put("pods", new Quantity("100"));
        status.setCapacity(quantityMap);
        status.setAllocatable(quantityMap);
        status.setImages(Collections.emptyList());
        node.setStatus(status);
        status.setConditions(conditions);
        final NodeSpec spec = new NodeSpec();
        spec.setUnschedulable(false);
        spec.setTaints(Collections.emptyList());
        node.setSpec(spec);
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(nodeName);
        meta.setLabels(labels);
        node.setMetadata(meta);
        return node;
    }

    private static Pod newPod(final String podName, final UUID uid, final String phase,
                              final Map<String, String> selectorLabels, final Map<String, String> labels) {
        final Pod pod = new Pod();
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp("1");
        meta.setNamespace("default");
        final PodSpec spec = new PodSpec();
        spec.setSchedulerName(Scheduler.SCHEDULER_NAME);
        spec.setPriority(0);
        spec.setNodeSelector(selectorLabels);

        final Container container = new Container();
        container.setName("pause");
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

    public static void runWorkload(final String[] args) throws Exception {
        final EmulatedCluster emulatedCluster = new EmulatedCluster();
        final Options options = new Options();

        options.addRequiredOption("n", "numNodes", true,
                "Number of nodes in experiment");
        options.addRequiredOption("f", "traceFile", true,
                "Trace file to use from k8s-scheduler/src/test/resources/");
        options.addRequiredOption("c", "cpuScaleDown", true,
                "Factor by which to scale down CPU resource demands for pods");
        options.addRequiredOption("m", "memScaleDown", true,
                "Factor by which to scale down Memory resource demands for pods");
        options.addRequiredOption("t", "timeScaleDown", true,
                "Factor by which to scale down arrival rate for pods");
        options.addRequiredOption("s", "startTimeCutOff", true,
                "N, where we replay first N seconds of the trace");
        options.addOption("p", "proportion", true,
                "P, from 0 to 100, indicating the proportion of pods that have affinity requirements");
        options.addOption("S", "scopeOn", false,
                "enable auto-scope in scheduler");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);
        final int numNodes = Integer.parseInt(cmd.getOptionValue("numNodes"));
        final String traceFile = cmd.getOptionValue("traceFile");
        final int cpuScaleDown = Integer.parseInt(cmd.getOptionValue("cpuScaleDown"));
        final int memScaleDown = Integer.parseInt(cmd.getOptionValue("memScaleDown"));
        final int timeScaleDown = Integer.parseInt(cmd.getOptionValue("timeScaleDown"));
        final int startTimeCutOff = Integer.parseInt(cmd.getOptionValue("startTimeCutOff"));
        final int affinityRequirementsProportion = Integer.parseInt(cmd.hasOption("proportion") ?
                cmd.getOptionValue("proportion") : "0");
        final boolean scopeOn = cmd.hasOption("scopeOn");
        assert affinityRequirementsProportion >= 0 && affinityRequirementsProportion <= 100;
        LOG.info("Running experiment with parameters: numNodes: {}, traceFile: {}, cpuScaleDown: {}, " +
                        "memScaleDown: {}, timeScaleDown: {}, startTimeCutOff: {}, proportion: {}, scopeOn: {}",
                numNodes, traceFile, cpuScaleDown, memScaleDown,
                timeScaleDown, startTimeCutOff, affinityRequirementsProportion, scopeOn);
        emulatedCluster.runTraceLocally(numNodes, traceFile, cpuScaleDown, memScaleDown, timeScaleDown,
                startTimeCutOff, affinityRequirementsProportion, scopeOn);
    }

    public static void main(final String[] args) throws Exception {
        runWorkload(args);
        System.exit(0); // without this, there are non-daemon threads that prevent JVM shutdown
    }
}