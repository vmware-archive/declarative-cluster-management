library(RSQLite)
library(bit64)
library(data.table)
library(ggplot2)
library(ggthemes)
library(rmarkdown)
library(xtable)

theme_Publication <- function(base_size=14) {
      (theme_foundation(base_size=base_size)
       + theme(plot.title = element_text(face = "bold",
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
               legend.margin = unit(0, "cm"),
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

savePlot <- function(plot, fileName, params, plotHeight, plotWidth, scale=TRUE) {
  if ("local" %in% params$kubeconfig) {
    ggsave(plot, file=paste(fileName, "Local", ".pdf", sep=""), 
           height= if(scale) plotHeight *1.5 else plotHeight, 
           width=plotWidth, units="in")
  } else {
   ggsave(plot, file=paste(fileName, ".pdf", sep=""), 
          height=plotHeight, 
          width=plotWidth, units="in")
  }
}

dir.create("plots")

conn <- dbConnect(RSQLite::SQLite(), "data.db")
params <- data.table(dbGetQuery(conn, "select * from params"))
schedulerTrace <- data.table(dbGetQuery(conn, "select * from scheduler_trace"))
podsOverTime <- data.table(dbGetQuery(conn, "select * from pods_over_time"))
dcmMetrics <- data.table(dbGetQuery(conn, "select * from dcm_metrics"))
dcmTableAccessLatency <- data.table(dbGetQuery(conn, "select * from dcm_table_access_latency"))
plotHeight=4
plotWidth=5
appCountCutOff=5000
fixedTimeScale=20

params$gitInfo <- paste(params$dcmGitBranch, substr(params$dcmGitCommitId, 0, 10), sep=":")
varyFExperiment <- NROW(unique(params$numNodes)) == 1
varyNExperiment <- NROW(unique(params$numNodes)) > 1

applyTheme <- function(ggplotObject) {
  ggplotObject +
  #scale_color_brewer(palette="Set1") +
  #theme_linedraw() +
  scale_colour_Publication() +
  scale_fill_Publication() +
  theme_Publication() +
  theme(legend.position="top", text = element_text(size=14))
}


#'
#' Change system names if required for anonymization or shortening
#'
dcmSchedulerAnonName <- "DCM"
defaultSchedulerName <- "default-scheduler"
params[scheduler == "dcm-scheduler"]$scheduler <- dcmSchedulerAnonName
schedulerTrace[scheduler == "dcm-scheduler"]$scheduler <- dcmSchedulerAnonName

params[scheduler == "default-scheduler" & percentageOfNodesToScoreValue == 100, 
       Scheme := sprintf("%s (%s%%)", scheduler, percentageOfNodesToScoreValue)]
params[scheduler == "default-scheduler" & percentageOfNodesToScoreValue == 0, 
       Scheme := sprintf("%s (sampling)", scheduler)]
params[scheduler == "DCM", Scheme := scheduler]
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
perPodSchedulingLatency <- schedulerTrace[as.numeric(appCount) >= appCountCutOff, list(Latency = bindTime, BatchSize = .N), 
                                          by=list(expId, batchId, scheduler)]
perPodSchedulingLatencyWithParams <- merge(perPodSchedulingLatency, params, by=c('expId'))
setnames(perPodSchedulingLatencyWithParams, old = "scheduler.x", new = "Scheduler")
if (varyFExperiment) {
  perPodSchedulingLatencyWithParams$color <- perPodSchedulingLatencyWithParams$Scheme
} else {
  perPodSchedulingLatencyWithParams[,color:=factor(paste("numNodes=", numNodes, sep=""), 
                                    levels=c("numNodes=500", "numNodes=5000", "numNodes=10000"))]
}

if(varyFExperiment) {
   perPodSchedulingLatencyWithParams[,af := factor(paste("F=", affinityProportion, "%", sep=""), 
                                                   levels = c("F=0%", "F=50%", "F=100%"))]
   schedulingLatencyEcdf100xPlot <- applyTheme(
      ggplot(perPodSchedulingLatencyWithParams,
             aes(x = Latency / 1e6 / BatchSize,  # To milliseconds
                 color = color), size= 1) +
      stat_ecdf(size = 1) +
      scale_linetype_manual(values = c("solid", "dotted")) +
      scale_x_log10(breaks=c(1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100)) +
      facet_grid(af ~ .  ) +
      coord_cartesian(xlim = c(1.1, 100)) +
      xlab("Scheduling Latency (ms)") +
      ylab("ECDF") +
      labs(col = "", linetype = "") 
   ) + guides(col = guide_legend(nrow = 1), linetype=guide_legend(keywidth = 1.6, nrow=2, byrow=TRUE))

   schedulingLatencyEcdf100xPlot
   savePlot(schedulingLatencyEcdf100xPlot, "plots/schedulingLatencyEcdfVaryFN=500Plot", params, plotHeight, plotWidth, scale=FALSE)
}


#'
#' Distribution of latency metrics collected by DCM, amortized over batch sizes.
#' Note: uses the first 1K batchIds only.
#'
countByBatchId <- perPodSchedulingLatencyWithParams[,.N,by=list(Scheduler,expId,batchId)]
dcmMetrics$dcmSolveTime <- as.numeric(dcmMetrics$dcmSolveTime) # convert to ns
dcmMetrics$presolveTime <- as.numeric(dcmMetrics$presolveTime * 1e9) # convert to ns
dcmMetrics$orToolsWallTime <- as.numeric(dcmMetrics$orToolsWallTime * 1e9) # convert to ns
dcmMetrics$orToolsUserTime <- as.numeric(dcmMetrics$orToolsUserTime * 1e9) # convert to ns
dcmMetricsWithBatchSizes <- merge(dcmMetrics, countByBatchId, by=c('expId', 'batchId'))

# Expand to repeat each row as many times as the batch size it represents
dcmMetricsWithBatchSizesExpanded <- dcmMetricsWithBatchSizes[rep(seq(.N), N)] 
longDcmLatencyMetricsWithBatchSizes <- melt(dcmMetricsWithBatchSizesExpanded, c("expId", "batchId", "N"), 
                                            measure=patterns("Time|Latency"))
longDcmLatencyMetricsWithBatchSizes$variableClean <- gsub("Latency|Total|Time", "", longDcmLatencyMetricsWithBatchSizes$variable)
longDcmLatencyMetricsWithBatchSizes <- merge(longDcmLatencyMetricsWithBatchSizes, params, by=c("expId"))
longDcmLatencyMetricsWithBatchSizes <- longDcmLatencyMetricsWithBatchSizes[variableClean != "orToolsUser"]
longDcmLatencyMetricsWithBatchSizes[variableClean == "orToolsWall"]$variableClean <- "orToolsTotal"

if (varyFExperiment) {
   dcmLatencyBreakdown100xPlot <- applyTheme(ggplot(longDcmLatencyMetricsWithBatchSizes[batchId > 100]) + 
      stat_ecdf(size=1, aes(x=value/1e6/N, col=variableClean, linetype=variableClean)) + 
      scale_x_log10(limits=c(0.01, 50)) +
      xlab("Latency (ms)") +
      ylab("ECDF") +
      facet_grid(factor(paste("F=", affinityProportion, "%", sep=""), levels=c("F=0%", "F=50%", "F=100%")) ~ .) +
      guides(linetype=guide_legend(keywidth = 2, keyheight = 1),
             colour=guide_legend(nrow=2, keywidth = 2, keyheight = 1))
   ) + theme(legend.title=element_blank())

   dcmLatencyBreakdown100xPlot
   savePlot(dcmLatencyBreakdown100xPlot, "plots/dcmLatencyBreakdownVaryFN=500Plot", params, plotHeight, plotWidth)
} else {
   dcmLatencyBreakdownPlot <- applyTheme(ggplot(longDcmLatencyMetricsWithBatchSizes[batchId > 100]) + 
      stat_ecdf(size=1, aes(x=value/1e6/N, col=variableClean, linetype=variableClean)) + 
      scale_x_log10() +
      xlab("Latency (ms)") +
      ylab("ECDF") +
      facet_grid(factor(paste("N=", numNodes, sep=""), levels=c("N=500", "N=5000", "N=10000")) ~ .) +
      guides(linetype=guide_legend(keywidth = 2, keyheight = 1),
             colour=guide_legend(nrow=2, keywidth = 2, keyheight = 1))
   ) + theme(legend.title=element_blank())

   dcmLatencyBreakdownPlot
   savePlot(dcmLatencyBreakdownPlot, "plots/dcmLatencyBreakdownF=100VaryNPlot", params, plotHeight, plotWidth, scale=FALSE)
}

#
#' Print model sizes to file
#
if (varyNExperiment) {
   longDcmModelMetricsWithBatchSizes <- melt(dcmMetricsWithBatchSizesExpanded, c("expId", "batchId", "N"), measure=patterns("variables"))
   longDcmModelMetricsWithBatchSizes$variableClean <- gsub("variables", "", longDcmModelMetricsWithBatchSizes$variable)
   longDcmModelMetricsWithBatchSizes <- merge(longDcmModelMetricsWithBatchSizes, params, by=c("expId"))

   result <- longDcmModelMetricsWithBatchSizes[batchId > 100 & grepl("Before", variableClean) 
                                          & !grepl("Objective", variableClean),
                                          list(Mean=round(mean(value))), by=list(numNodes)]
   colnames(result)[1] <- "#Nodes"
   colnames(result)[2] <- "#Variables"
   print(xtable(result, type = "latex", caption="Average number of model variables in the computed
   optimization models as a function of cluster size"), 
         file = "plots/modelSizeTable.tex",
         include.rownames=FALSE)
}
