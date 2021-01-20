/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

public class SolverException extends RuntimeException {
    private final String reason;

    public SolverException(final String reason) {
        super(reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}