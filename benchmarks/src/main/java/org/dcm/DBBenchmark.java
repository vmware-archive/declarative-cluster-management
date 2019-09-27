/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;

public class DBBenchmark {

    public static void main(String[] args) throws IOException, RunnerException {
        Options opts = new OptionsBuilder()
                .include(".*")
//                .addProfiler(StackProfiler.class, "lines=3")
                .warmupIterations(1)
                .measurementTime(TimeValue.milliseconds(1))
                .measurementIterations(1)
                .mode(Mode.Throughput)
                .shouldDoGC(true)
//                .result("profiling-result.csv").resultFormat(ResultFormatType.CSV)
                .forks(1)
                .build();

        new Runner(opts).run();
    }

    @Benchmark
    public void dbTest() {
        callAnotherFunction();
        System.out.println("Apples");
    }


    public void callAnotherFunction() {
        System.out.println("Meow");
    }
}
