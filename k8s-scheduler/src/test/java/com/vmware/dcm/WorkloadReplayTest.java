/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.junit.jupiter.api.Test;

public class WorkloadReplayTest {

    @Test
    public void runTest() throws Exception {
        final String[] args =
                {"-n", "50000", "-f", "v2-cropped.txt", "-c", "100", "-m", "200", "-t", "100", "-s", "100"};
        EmulatedCluster.main(args);
    }

    @Test
    public void runTestScope() throws Exception {
        final String[] args =
                {"-n", "50000", "-f", "v2-cropped.txt", "-c", "100", "-m", "200", "-t", "100", "-s", "100", "-p",
                        "100", "-S"};
        EmulatedCluster.main(args);
    }
}
