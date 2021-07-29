/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An exception thrown when invoking the solver. Typically used to convey infeasibility or some other error.
 *
 * Optionally returns an UNSAT core, if the solver supports it.
 */
public class SolverException extends RuntimeException {
    private final String reason;
    private final List<String> core;

    public SolverException(final String reason) {
        super(reason);
        this.reason = reason;
        this.core = Collections.emptyList();
    }

    public SolverException(final String reason, final List<String> core) {
        super(reason + " " + new HashSet<>(core));
        this.reason = reason;
        this.core = core;
    }

    public String reason() {
        return reason;
    }

    public Set<String> core() {
        return new HashSet<>(core);
    }
}