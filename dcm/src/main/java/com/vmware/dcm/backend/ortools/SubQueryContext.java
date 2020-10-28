/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

// TODO: consolidate into TranslationContext
class SubQueryContext {
    private final String subQueryName;

    SubQueryContext(final String subQueryName) {
        this.subQueryName = subQueryName;
    }

    public String getSubQueryName() {
        return subQueryName;
    }
}
