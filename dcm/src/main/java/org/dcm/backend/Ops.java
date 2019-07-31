/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import com.google.ortools.sat.CpModel;

import java.util.List;

public class Ops {
    private final CpModel model;

    public Ops(final CpModel model) {
        this.model = model;
    }

    public int sum(final List<Integer> data) {
        int ret = 0;
        for (final Integer d: data) {
            ret += d;
        }
        return ret;
    }
}
