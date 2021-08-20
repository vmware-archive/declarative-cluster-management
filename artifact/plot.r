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

savePlot <- function(plot, fileName, suffix, params, plotHeight, plotWidth, scale=TRUE) {
    if ("local" %in% params$kubeconfig) {
        ggsave(plot, file=paste(fileName, suffix, "Local", ".pdf", sep=""), 
               height = plotHeight,
               width=plotWidth, units="in")
    } else {
        ggsave(plot, file=paste(fileName, suffix, ".pdf", sep=""), 
               height=plotHeight, 
               width=plotWidth, units="in")
    }
}

#' 
#' Configuration
#'
args <- commandArgs(TRUE)
db <- args[1]
mode <- args[2]

dir.create("plots")

conn <- dbConnect(RSQLite::SQLite(), db)
params <- data.table(dbGetQuery(conn, "select * from params"))
schedulerTrace <- data.table(dbGetQuery(conn, "select * from scheduler_trace"))
podsOverTime <- data.table(dbGetQuery(conn, "select * from pods_over_time"))
dcmMetrics <- data.table(dbGetQuery(conn, "select * from dcm_metrics"))
dcmTableAccessLatency <- data.table(dbGetQuery(conn, "select * from dcm_table_access_latency"))
scopeMetrics <- data.table(dbGetQuery(conn, "select * from scope_fraction"))

if (mode == "full") {
    appCountCutOff=0 ## FIXME: increase when exceptions are resolved
    batchIdMin=3
} else {
    appCountCutOff=0
    batchIdMin=3
}
fixedTimeScale=20
plotHeight=5
plotWidth=7

params$gitInfo <- paste(params$dcmGitBranch, substr(params$dcmGitCommitId, 0, 10), sep=":")
varyFExperiment <- NROW(unique(params$numNodes)) == 1
varyNExperiment <- NROW(unique(params$numNodes)) > 1

applyTheme <- function(ggplotObject) {
    ggplotObject +
    ## scale_color_brewer(palette="Set1") +
    ## theme_linedraw() +
    scale_colour_Publication() +
    scale_fill_Publication() +
    theme_Publication() +
    theme(legend.position="top", text = element_text(size=14))
}


#'
#' Change system names if required for anonymization or shortening
#'
dcmSchedulerName <- "DCM"
dcmScopedSchedulerName <- "DCM-scoped"
defaultSchedulerName <- "default-scheduler"
params[scheduler == "dcm-scheduler"]$scheduler <- dcmSchedulerName
params[scheduler == "dcm-scheduler-scoped"]$scheduler <- dcmScopedSchedulerName

params[scheduler == "default-scheduler" & percentageOfNodesToScoreValue == 100, 
       Scheme := sprintf("%s (%s%%)", scheduler, percentageOfNodesToScoreValue)]
params[scheduler == "default-scheduler" & percentageOfNodesToScoreValue == 0, 
       Scheme := sprintf("%s (sampling)", scheduler)]
params[scheduler == "DCM", Scheme := scheduler]
params[scheduler == "DCM-scoped", Scheme := scheduler]
#' Display experiment metadata
print(params)

#' Boxplot custom function
bp.vals <- function(x, probs=c(0.01, 0.25, 0.5, 0.75, .99)) {
    r <- quantile(x, probs=probs , na.rm=TRUE)
    names(r) <- c("ymin", "lower", "middle", "upper", "ymax")
    r
}

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
perPodSchedulingLatencyWithParams <- merge(perPodSchedulingLatency, params, by=c('expId'))

if (varyFExperiment) {
    perPodSchedulingLatencyWithParams$color <- perPodSchedulingLatencyWithParams$Scheme
} else {
    perPodSchedulingLatencyWithParams[,color:=factor(paste("numNodes=", numNodes, sep=""),
      levels=sapply(params$numNodes[1:3], function(n) paste(c("numNodes=", n), collapse = "")))]
}

#'
#' Distribution of scope fractions used.
#'
scopeMetrics$scopePercent = scopeMetrics$scopeFraction * 100
scopeMetricsWithParams <- merge(scopeMetrics[, scopePercent, by=list(expId)], params, by=c('expId'))

if (varyFExperiment) {
    scopeMetricsPdfPlot <- applyTheme(
        ggplot(scopeMetricsWithParams,
               aes(x = scopePercent), size = 1) +
        geom_density(alpha = 0.4, fill = "#fdb462") + # TODO: play with that later
        facet_grid(factor(paste("F=", affinityProportion, "%", sep=""), 
                          levels = c("F=0%", "F=50%", "F=100%")) ~ ., scales="free") +
        xlab("Scoped nodes percent (%)") +
        ylab("PDF") +
        labs(col = "", linetype = "") 
    ) + guides(col = guide_legend(nrow = 1), linetype=guide_legend(keywidth = 1.6, nrow=2, byrow=TRUE))

    scopeMetricsPdfPlot
    savePlot(scopeMetricsPdfPlot, "plots/scopeMetricsPdfPlotVaryFN=",
             scopeMetricsWithParams$numNodes[1], params, plotHeight, plotWidth/2, scale=FALSE)
} else {
    scopeMetricsPdfPlot <- applyTheme(
        ggplot(scopeMetricsWithParams,
               aes(x = scopePercent), size = 1) +
        geom_density(alpha = 0.4, fill = "#fdb462") + # TODO: play with that later
        ## geom_histogram() +
        ## geom_vline(aes(xintercept=mean(scopePercent)), linetype="dashed", size=1) +
        facet_grid(factor(paste("N=", numNodes, sep=""), 
                          levels = sapply(params$numNodes[1:3],
                                          function(n) paste(c("N=", n), collapse = ""))) ~ .,
                   scales="free") +
        xlab("Scoped nodes percent (%)") +
        ylab("PDF") +
        labs(col = "", linetype = "") 
    ) + guides(col = guide_legend(nrow = 1), linetype=guide_legend(keywidth = 1.6, nrow=2, byrow=TRUE))

    scopeMetricsPdfPlot
    savePlot(scopeMetricsPdfPlot, "plots/scopeMetricsPdfPlotVaryNF=",
             scopeMetricsWithParams$numNodes[1], params, plotHeight, plotWidth/2, scale=FALSE)    
}

if (varyFExperiment) {
    schedulingLatencyEcdf100xPlot <- applyTheme(
        ggplot(perPodSchedulingLatencyWithParams,
               aes(x = Latency / 1e6 / BatchSize,  # To milliseconds
                   color = color), size = 1) +
        stat_ecdf(size = 1) +
        scale_linetype_manual(values = c("solid", "dotted")) +
        scale_x_log10() +
        ## scale_x_log10(breaks=c(1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100)) +
        facet_grid(factor(paste("F=", affinityProportion, "%", sep=""), 
                          levels = c("F=0%", "F=50%", "F=100%")) ~ . ) +
        ## coord_cartesian(xlim = c(1.1, 100)) +
        xlab("Scheduling Latency (ms)") +
        ylab("ECDF") +
        labs(col = "", linetype = "") 
    ) + guides(col = guide_legend(nrow = 1), linetype=guide_legend(keywidth = 1.6, nrow=2, byrow=TRUE))

    schedulingLatencyEcdf100xPlot
    savePlot(schedulingLatencyEcdf100xPlot, "plots/schedulingLatencyEcdfVaryFN=",
             perPodSchedulingLatencyWithParams$numNodes[1], params, plotHeight, plotWidth, scale=FALSE)
}

#'
#' Distribution of latency metrics collected by DCM, amortized over batch sizes.
#' Note: uses the first 1K batchIds only.
#'
countByBatchId <- perPodSchedulingLatencyWithParams[,.N,by=list(expId,batchId)]
dcmMetrics$dcmSolveTime <- as.numeric(dcmMetrics$dcmSolveTime)
dcmMetrics$modelCreationLatency <- as.numeric(dcmMetrics$modelCreationLatency)
dcmMetrics$scopeLatency <- as.numeric(dcmMetrics$scopeLatency)
dcmMetrics$databaseLatencyTotal <- as.numeric(dcmMetrics$databaseLatencyTotal)
dcmMetrics$presolveTime <- as.numeric(dcmMetrics$presolveTime * 1e9) # convert to ns
dcmMetrics$orToolsWallTime <- as.numeric(dcmMetrics$orToolsWallTime * 1e9) # convert to ns
dcmMetrics$orToolsUserTime <- as.numeric(dcmMetrics$orToolsUserTime * 1e9) # convert to ns
dcmMetrics[scopeLatency == 0]$scopeLatency <- NA # ommit line of zeros when scope is off
dcmMetricsWithBatchSizes <- merge(dcmMetrics, countByBatchId, by=c('expId', 'batchId'))

# Expand to repeat each row as many times as the batch size it represents
dcmMetricsWithBatchSizesExpanded <- dcmMetricsWithBatchSizes[rep(seq(.N), N)]
longDcmLatencyMetricsWithBatchSizes <- melt(dcmMetricsWithBatchSizesExpanded, c("expId", "batchId", "N"),
                                            measure=patterns("Time|Latency"))
longDcmLatencyMetricsWithBatchSizes$variableClean <- gsub("Latency|Total|Time", "",
                                                          longDcmLatencyMetricsWithBatchSizes$variable)
longDcmLatencyMetricsWithBatchSizes <- merge(longDcmLatencyMetricsWithBatchSizes, params, by=c("expId"))
longDcmLatencyMetricsWithBatchSizes <- longDcmLatencyMetricsWithBatchSizes[variableClean != "orToolsUser"]
longDcmLatencyMetricsWithBatchSizes[variableClean == "orToolsWall"]$variableClean <- "orToolsTotal"

if (varyFExperiment) {
    dcmLatencyBreakdown100xPlot <- applyTheme(
        ggplot(longDcmLatencyMetricsWithBatchSizes[batchId >= batchIdMin]) +
        stat_ecdf(size=1, aes(x=value/1e6/N, col=variableClean, linetype=variableClean)) +
        scale_x_log10() + 
        ## coord_cartesian(xlim = c(0.01, 50)) +
        xlab("Latency (ms)") +
        ylab("ECDF") +
        facet_grid(factor(paste("F=", affinityProportion, "%", sep=""),
                          levels=c("F=0%", "F=50%", "F=100%")) ~ scheduler ) +
        guides(linetype=guide_legend(keywidth = 2, keyheight = 1),
               colour=guide_legend(nrow=2, keywidth = 2, keyheight = 1))
    ) + theme(legend.title=element_blank())

    dcmLatencyBreakdown100xPlot
    savePlot(dcmLatencyBreakdown100xPlot, "plots/dcmLatencyBreakdownVaryFN=",
             longDcmLatencyMetricsWithBatchSizes$numNodes[1], params, plotHeight, plotWidth)
} else {
    dcmLatencyBreakdownPlot <- applyTheme(
        ggplot(longDcmLatencyMetricsWithBatchSizes[batchId >= batchIdMin]) +
        stat_ecdf(size=1, aes(x=value/1e6/N, col=variableClean, linetype=variableClean)) + 
        scale_x_log10() +
        xlab("Latency (ms)") +
        ylab("ECDF") +
        facet_grid(factor(paste("N=", numNodes, sep=""),
                          levels=sapply(params$numNodes[1:3],
                                        function(n) paste(c("N=", n), collapse = ""))) ~ scheduler ) +
        guides(linetype=guide_legend(keywidth = 2, keyheight = 1),
               colour=guide_legend(nrow=2, keywidth = 2, keyheight = 1))
    ) + theme(legend.title=element_blank())

    dcmLatencyBreakdownPlot
    savePlot(dcmLatencyBreakdownPlot, "plots/dcmLatencyBreakdownVaryNF=",
             longDcmLatencyMetricsWithBatchSizes$affinityProportion[1], params, plotHeight, plotWidth, scale=FALSE)
}

#
#' Print model sizes to file
#
if (varyNExperiment) {
    longDcmModelMetricsWithBatchSizes <- melt(dcmMetricsWithBatchSizesExpanded,
                                              c("expId", "batchId", "N"), measure=patterns("variables"))
    longDcmModelMetricsWithBatchSizes$variableClean <- gsub("variables", "",
                                                            longDcmModelMetricsWithBatchSizes$variable)
    longDcmModelMetricsWithBatchSizes <- merge(longDcmModelMetricsWithBatchSizes, params, by=c("expId"))
    
    result <- longDcmModelMetricsWithBatchSizes[batchId >= batchIdMin & grepl("Before", variableClean)
                                                & !grepl("Objective", variableClean),
                                                list(Mean=round(mean(value))), by=list(numNodes)]

    colnames(result)[1] <- "#Nodes"
    colnames(result)[2] <- "#Variables"
    print(xtable(result, type = "latex", caption="Average number of model variables in the computed
      optimization models as a function of cluster size"), 
      file = "plots/modelSizeTable.tex",
      include.rownames=FALSE)
}
