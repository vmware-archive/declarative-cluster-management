library(RSQLite)
library(bit64)
library(data.table)
library(ggplot2)
library(ggthemes)
library(rmarkdown)
library(xtable)

theme_Publication <- function(base_size=14) {
    (theme_foundation(base_size=base_size) +
     theme(plot.title = element_text(face = "bold",
                                     size = rel(1.2), hjust = 0.5),
           text = element_text(),
           panel.background = element_rect(colour = NA),
           plot.background = element_rect(colour = NA),
           panel.border = element_rect(colour = NA),
           axis.title = element_text(face = "bold",size = rel(1)),
           axis.title.y = element_text(angle=90,vjust =2),
           axis.title.x = element_text(vjust = -0.2),
           axis.text = element_text(), 
           axis.line = element_line(colour="black"),
           axis.ticks = element_line(),
           panel.grid.major = element_line(colour="#f0f0f0"),
           panel.grid.minor = element_blank(),
           legend.key = element_rect(colour = NA),
           legend.position = "bottom",
           legend.direction = "horizontal",
           legend.key.size= unit(0.2, "cm"),
           legend.spacing = unit(0, "cm"),
           legend.title = element_text(face="italic"),
           plot.margin=unit(c(10,5,5,5),"mm"),
           strip.background=element_rect(colour="#f0f0f0",fill="#f0f0f0"),
           strip.text = element_text(face="bold")
           ))
    
}

scale_fill_Publication <- function(...){
    library(scales)
    discrete_scale("fill", "Publication",
                   manual_pal(values = c("#386cb0", "#fdb462", "#7fc97f", "#ef3b2c", "#662506",
                                         "#a6cee3", "#fb9a99", "#984ea3", "#ffff33")), ...)
}

scale_colour_Publication <- function(...){
    library(scales)
    discrete_scale("colour", "Publication",
                   manual_pal(values = c("#386cb0", "#fdb462", "#7fc97f", "#ef3b2c",
                                         "#662506","#a6cee3","#fb9a99","#984ea3","#ffff33")), ...)
}

applyTheme <- function(ggplotObject) {
    ggplotObject +
    ## scale_color_brewer(palette="Set1") +
    ## theme_linedraw() +
    scale_colour_Publication() +
    scale_fill_Publication() +
    theme_Publication() +
    theme(legend.position="top", text = element_text(size=14))
}

savePlot <- function(plot, fileName, rows, params, plotHeight, plotWidth, scale=TRUE) {
    fileName = paste(outdir, "/", fileName, sep="")
    if (length(rows) > 3) {
        height = 2 * plotHeight
    } else {
        height = plotHeight
    }
    if ("local" %in% params$kubeconfig) {
        ggsave(plot, file=paste(fileName, "Local", ".pdf", sep=""),
               height=height, width=plotWidth, units="in")
    } else {
        ggsave(plot, file=paste(fileName, ".pdf", sep=""),
               height=height, width=plotWidth, units="in")
    }
}


#' 
#' Configuration
#'
args <- commandArgs(TRUE)
mode <- args[1]
db <- args[2]
outdir <- args[3]


conn <- dbConnect(RSQLite::SQLite(), db)
params <- data.table(dbGetQuery(conn, "select * from params"))
podEvents <- data.table(dbGetQuery(conn, "select * from pod_events"))
schedulerTrace <- data.table(dbGetQuery(conn, "select * from scheduler_trace"))
dcmMetrics <- data.table(dbGetQuery(conn, "select * from dcm_metrics"))
dcmTableAccessLatency <- data.table(dbGetQuery(conn, "select * from dcm_table_access_latency"))
scopeMetrics <- data.table(dbGetQuery(conn, "select * from scope_fraction"))

if (mode == "full") {
    appCountCutOff=0 ## FIXME: increase when/if we have more apps with no exceptions
    batchIdMin=3
} else {
    appCountCutOff=0
    batchIdMin=3
}
plotHeight=5
plotWidth=7

params$gitInfo <- paste(params$dcmGitBranch, substr(params$dcmGitCommitId, 0, 10), sep=":")


#'
#' Change system names if required for anonymization or shortening
#'
dcmSchedulerName <- "DCM"
dcmScopedSchedulerName <- "AutoScope"
defaultSchedulerName <- "default-scheduler"
params[scheduler == "dcm-scheduler"]$scheduler <- dcmSchedulerName
params[scheduler == "dcm-scheduler-scoped"]$scheduler <- dcmScopedSchedulerName
params[scheduler == "DCM", Scheme := scheduler]
params[scheduler == "AutoScope", Scheme := scheduler]

#'
#' ## Scheduling latency and batch sizes
#'
#' This is the latency to compute a scheduling decision. For DCM, given that it
#' schedules in batches, the latency per pod in a batch is computed as the latency 
#' for scheduling a batch divided by the batch size. The data frames represent
#' latency measurements in nanoseconds, which we convert to milliseconds when plotting.
schedulerTrace$appCount <- tstrsplit(schedulerTrace$podName, "-")[2]
perPodSchedulingLatency <- schedulerTrace[as.numeric(appCount) >= appCountCutOff,
                                          list(Latency = bindTime, BatchSize = .N), 
                                          by=list(expId, batchId)]
perPodSchedulingLatency$normLatency <- perPodSchedulingLatency[, Latency / BatchSize]
perPodSchedulingLatencyWithParams <- merge(perPodSchedulingLatency, params, by=c('expId'))


#'
#' Latency vs cluster size.
#'
## (mean, std) in normLatency[,1], normLatency[,2]
perExpAvgSchedulingLatency <- aggregate(normLatency ~ expId, perPodSchedulingLatency,
                                        function(x) c(mean = mean(x), sd = sd(x)))
perExpAvgSchedulingLatencyWithParams <- merge(perExpAvgSchedulingLatency, params, by=c('expId'))

scalabilityClusterSizePlot <- applyTheme(
    ggplot(perExpAvgSchedulingLatencyWithParams,
           aes(x = numNodes,
               y = normLatency[,1] / 1e6, # To milliseconds
               color = Scheme)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = (normLatency[,1]-normLatency[,2]) / 1e6,
                      ymax = (normLatency[,1]+normLatency[,2]) / 1e6,
                      width = .02)) +
    scale_x_log10(breaks = perExpAvgSchedulingLatencyWithParams$numNodes) +
    ## coord_cartesian(xlim = c(1.1, 100)) +
    xlab("Cluster size") +
    ylab("Average scheduling latency (ms)") +
    labs(col = "", linetype = "")
)
savePlot(scalabilityClusterSizePlot, "scalabilityClusterSize",
         0, params, plotHeight, plotWidth, scale=FALSE)


#'
#' Latency ECDFs.
#'
for (facetOption in c("Nodes", "Scenarios", "NodesXScenarios")) {
    switch(facetOption,
           "Nodes" = {
               facetCol <- unique(perPodSchedulingLatencyWithParams$numNodes)
               nonFacetColName <- ""
               nonFacetCol <- unique(perPodSchedulingLatencyWithParams$scenario)
               nonFacetColPick <- nonFacetCol[length(nonFacetCol)]
               data <- perPodSchedulingLatencyWithParams[scenario == nonFacetColPick]
           },
           "Scenarios" = {
               facetCol <- unique(perPodSchedulingLatencyWithParams$scenario)
               nonFacetColName <- "nodes"
               nonFacetCol <- unique(perPodSchedulingLatencyWithParams$numNodes)
               nonFacetColPick <- nonFacetCol[length(nonFacetCol)]
               data <- perPodSchedulingLatencyWithParams[numNodes == nonFacetColPick]
           },
           "NodesXScenarios" = {
               facetCol <- unique(perPodSchedulingLatencyWithParams$numNodes)
               nonFacetColName <- ""
               nonFacetColPick <- ""
               data <- perPodSchedulingLatencyWithParams
           })
    schedulingLatencyEcdfPlot <- applyTheme(
        ggplot(data,
               aes(x = normLatency / 1e6, # To milliseconds
                   color = Scheme)) +
        stat_ecdf() +
        switch(facetOption,
               "Nodes" = {
                   facet_grid(factor(paste(numNodes, "nodes"),
                                     levels = sapply(unique(numNodes), function(n) paste(n, "nodes")))
                              ~ .)
               },
               "Scenarios" = {
                   facet_grid(scenario ~ .)
               },
               "NodesXScenarios" = {
                   facet_grid(factor(paste(numNodes, "nodes"),
                                     levels = sapply(unique(numNodes), function(n) paste(n, "nodes")))
                              ~ scenario)
               }) +
        scale_x_log10() +
        xlab("Latency (ms)") +
        ylab("ECDF") +
        labs(col = "", linetype = "", subtitle=paste(nonFacetColPick, nonFacetColName))
    )
    savePlot(schedulingLatencyEcdfPlot, paste("latencyEcdfAllPlotFacetOn", facetOption, sep=""),
             facetCol, params, plotHeight, plotWidth)
}

#'
#' Latency breakdown
#'
#' TODO: comment on bathcIdMin
countByBatchId <- perPodSchedulingLatencyWithParams[,.N,by=list(expId,batchId)]
dcmMetrics$dcmSolveTime <- as.numeric(dcmMetrics$dcmSolveTime)
dcmMetrics$modelCreationLatency <- as.numeric(dcmMetrics$modelCreationLatency)
dcmMetrics$scopeLatency <- as.numeric(dcmMetrics$scopeLatency)
dcmMetrics$databaseLatencyTotal <- as.numeric(dcmMetrics$databaseLatencyTotal)
dcmMetrics$presolveTime <- as.numeric(dcmMetrics$presolveTime * 1e9) # convert to ns
dcmMetrics$orToolsWallTime <- as.numeric(dcmMetrics$orToolsWallTime * 1e9) # convert to ns
dcmMetrics$orToolsUserTime <- as.numeric(dcmMetrics$orToolsUserTime * 1e9) # convert to ns
dcmMetrics[scopeLatency == 0]$scopeLatency <- NA # ommit line of zeros when scope is off
dcmMetrics <- merge(dcmMetrics, countByBatchId, by=c('expId', 'batchId'))

# Expand to repeat each row as many times as the batch size it represents
dcmMetricsExpanded <- dcmMetrics[rep(seq(.N), N)]
longDcmLatencyMetrics <- melt(dcmMetricsExpanded, c("expId", "batchId", "N"),
                                            measure=patterns("Time|Latency"))
longDcmLatencyMetrics$variableClean <- gsub("Latency|Total|Time", "",
                                                          longDcmLatencyMetrics$variable)
longDcmLatencyMetrics <- merge(longDcmLatencyMetrics, params, by=c("expId"))
longDcmLatencyMetrics <- longDcmLatencyMetrics[variableClean != "orToolsUser"]
longDcmLatencyMetrics[variableClean == "orToolsWall"]$variableClean <- "orToolsTotal"

for (facetOption in c("Nodes", "Scenarios")) {
    switch(facetOption,
           "Nodes" = {
               facetCol <- unique(longDcmLatencyMetrics$numNodes)
               nonFacetColName <- ""
               nonFacetCol <- unique(longDcmLatencyMetrics$scenario)
               nonFacetColPick <- nonFacetCol[length(nonFacetCol)]
               data <- longDcmLatencyMetrics[scenario == nonFacetColPick]
           },
           "Scenarios" = {
               facetCol <- unique(longDcmLatencyMetrics$scenario)
               nonFacetColName <- "nodes"
               nonFacetCol <- unique(longDcmLatencyMetrics$numNodes)
               nonFacetColPick <- nonFacetCol[length(nonFacetCol)]
               data <- longDcmLatencyMetrics[numNodes == nonFacetColPick]
           })

    dcmLatencyBreakdown100xPlot <- applyTheme(
        ggplot(data[batchId >= batchIdMin]) +
        stat_ecdf(size=1, aes(x=value/1e6/N, col=variableClean, linetype=variableClean)) +
        switch(facetOption,
               "Nodes" = {
                   facet_grid(factor(paste(numNodes, "nodes"),
                                     levels = sapply(unique(numNodes), function(n) paste(n, "nodes")))
                              ~ scheduler)
               },
               "Scenarios" = {
                   facet_grid(scenario ~ scheduler)
               }) +
        scale_x_log10() + 
        ## coord_cartesian(xlim = c(0.01, 50)) +
        xlab("Latency (ms)") +
        ylab("ECDF") +
        labs(subtitle=paste(nonFacetColPick, nonFacetColName)) +
        guides(linetype=guide_legend(keywidth = 2, keyheight = 1),
               colour=guide_legend(nrow=2, keywidth = 2, keyheight = 1))
    ) + theme(legend.title=element_blank())
    savePlot(dcmLatencyBreakdown100xPlot, paste("dcmLatencyBreakdownPlotFacetOn", facetOption, sep=""),
             facetCol, params, plotHeight, plotWidth)
}

#'
#' Evolution of #Variables
#'
dcmMetricsExpandedWithParams <- merge(dcmMetricsExpanded, params, by=c("expId"))
solverVariablesPlot <- applyTheme(
    ggplot(dcmMetricsExpandedWithParams,
           aes(x = batchId,
               y = variablesAfterPresolve,
               color = Scheme)) +
    geom_line() +
    facet_grid(factor(paste(numNodes, "nodes"),
                      levels = sapply(unique(numNodes), function(n) paste(n, "nodes")))
               ~ scenario) +
    xlab("batch progress") +
    ylab("# Variables for incoming batches") +
    labs(col = "", linetype = "")
)
savePlot(solverVariablesPlot, "solverVariablesPlot",
         unique(dcmMetricsExpandedWithParams$numNodes), params, plotHeight, plotWidth)

#'
#' Events barplots
#'
podEventsWithParams <- merge(podEvents, params, by=c("expId"))
podEventsBarPlot <- applyTheme(
    ggplot(podEventsWithParams,
           aes(x = event)) +
    geom_bar(stat="count", fill="brown") +
    scale_x_discrete(guide = guide_axis(angle = 45)) +
    geom_text(aes(label = ..count..), stat="count", vjust=0, colour="black") +
    facet_grid(factor(paste(numNodes, "nodes"),
                      levels = sapply(unique(numNodes), function(n) paste(n, "nodes")))
               ~ scenario)
)
savePlot(podEventsBarPlot, "podEventsBarPlot",
         0, params, plotHeight*2, plotWidth*2)


## TODO: export table with avg vars
## TODO: need to generate scope metrics in new codebase

