/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.jupiter.api.AfterEach;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class UnbindTest {
    @Nullable
    private KubernetesServer server;

    @AfterEach
    void tearDown() {
        assertNotNull(server);
        server.after();
    }

}
