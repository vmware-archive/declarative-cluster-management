/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Optional;

enum JavaType {
    IntVar("IntVar"),
    String("String"),
    Boolean("Boolean"),
    Long("Long"),
    Integer("Integer"),
    Float("Float"),
    ObjectArray("Object[]"),
    ListOfIntVar("List", IntVar),
    ListOfInteger("List", Integer),
    ListOfLong("List", Long),
    ListOfBool("List", Boolean),
    ListOfString("List", String),
    ListOfObjectArray("List", ObjectArray);

    private final String typeString;
    @Nullable private final JavaType innerType;

    JavaType(final String typeString) {
        this.typeString = typeString;
        this.innerType = null;
    }

    JavaType(final String typeString, final JavaType type) {
        this.typeString = typeString;
        this.innerType = type;
    }

    public String typeString() {
        if (this.innerType != null) {
            return java.lang.String.format("List<%s>", innerType.typeString());
        }
        return typeString;
    }

    public static JavaType listType(final JavaType innerType) {
        switch (innerType) {
            case IntVar:
                return ListOfIntVar;
            case Integer:
                return ListOfInteger;
            case Long:
                return ListOfLong;
            case String:
                return ListOfString;
            case Boolean:
                return ListOfBool;
            case ObjectArray:
                return ListOfObjectArray;
            default:
                throw new IllegalArgumentException(innerType.toString());
        }
    }

    public Optional<JavaType> innerType() {
        return Optional.ofNullable(innerType);
    }
}