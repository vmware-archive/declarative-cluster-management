/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

class JavaTypeList {
    private final List<JavaType> typeList;

    JavaTypeList(final List<JavaType> typeList) {
        Preconditions.checkArgument(!typeList.isEmpty(), "Empty type list detected");
        this.typeList = typeList;
    }

    boolean contains(final JavaType type) {
        return typeList.contains(type);
    }

    @Override
    public String toString() {
        return typeList.stream().map(JavaType::typeString).collect(Collectors.joining(", "));
    }
}
