/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

enum JavaType {
    IntVar("IntVar"),
    String("String"),
    Boolean("Boolean"),
    Long("Long"),
    Integer("Integer"),
    Float("Float"),
    ObjectArray("Object[]");

    private final String typeString;

    JavaType(final String typeString) {
        this.typeString = typeString;
    }

    public String typeString() {
        return typeString;
    }
}