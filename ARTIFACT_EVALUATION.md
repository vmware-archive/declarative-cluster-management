# Requirements

- Docker: https://www.docker.com/


# Artifact goals

We will reproduce key results from the Kubernetes scheduler evaluation in section 6.1.
Given that some of the experiments were run on a 500 node AWS cluster in the paper, we 
will use the simulation set up described in the "effect of increased cluster sizes" 
experiment to run DCM on a single host (we cannot run the baseline Kubernetes scheduler 
this way). We will reproduce Table 1, and figures 13, 14 and 15 for DCM.


# Instructions to produce results

1. Run the following command to launch a docker image with all the dependencies installed. The image is
   roughly 2GB in size, and might take some time to download:

 ```
  $: sudo docker run -it lalithsuresh/dcm-osdi-artifact bash
 ```

2. Once in the docker image, run the following command to clone the DCM respository's artifact branch:

 ```
  $: git clone http://github.com/vmware/declarative-cluster-management --single-branch --branch artifact
 ```

3. Run the following script, which will build the DCM binaries and run the experiments (takes roughly 30-40 minutes,
   on a MacBook Pro with a 2.6 GHz 6-Core Intel Core i7 CPU and 32 GB RAM):

 ```
  $: cd declarative-cluster-management
  $: ./run_emulation.sh 20000
 ```
 
 Note: the results for the paper were produced with longer runs, equivalent to using `./run_emulation.sh 200000`, 
 which takes roughly 3-4 hours to execute. You'll find the shorter run above produces similar plots.

4. Once the command completes, confirm that it has created a `plots/` folder with the following files:
   * schedulingLatencyEcdfVaryFN=500PlotLocal.pdf (Figure 13, DCM)
   * dcmLatencyBreakdownVaryFN=500PlotLocal.pdf   (Figure 14)
   * dcmLatencyBreakdownF=100VaryNPlotLocal.pdf   (Figure 15)
   * modelSizeTable.tex                           (Table 1)

5. To view the above files, open another terminal and copy out the results from the docker image (note: the
   below command runs on your host, not inside the docker image. It assumes there is only one instance
   of the docker image running):

 ```
  $: ARTIFACT_IMAGE=`sudo docker ps | grep "dcm-osdi-artifact" | awk '{print $1}'`
  $: sudo docker cp $ARTIFACT_IMAGE:/declarative-cluster-management/plots .
 ```
