/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import org.junit.jupiter.api.Test;

public class WorkloadReplayTest {

    @Test
    public void runTest() throws Exception {
        final String[] args = {"-n", "500", "-f", "test-data.txt", "-c", "100", "-m", "200", "-t", "100", "-s", "0"};
        EmulatedCluster.runWorkload(args);
    }
}
