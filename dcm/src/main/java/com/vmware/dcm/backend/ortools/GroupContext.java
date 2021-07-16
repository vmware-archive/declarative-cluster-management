/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.google.common.base.CaseFormat;
import com.vmware.dcm.compiler.ir.GroupByQualifier;

class GroupContext {
    private final GroupByQualifier qualifier;
    private final String tempTableName;
    private final String groupViewName;

    GroupContext(final GroupByQualifier qualifier, final String tempTableName, final String groupViewName) {
        this.qualifier = qualifier;
        this.tempTableName = tempTableName;
        this.groupViewName = groupViewName;
    }

    public GroupByQualifier getQualifier() {
        return qualifier;
    }

    public String getTempTableName() {
        return tempTableName;
    }

    public String getGroupViewName() {
        return groupViewName;
    }

    public String getGroupName() {
        return camelCase(groupViewName) + "Group";
    }

    public String getGroupDataName() {
        return camelCase(groupViewName) + "Data";
    }

    public String getGroupDataTupleName() {
        return getGroupDataName() + "Tuple";
    }

    private String camelCase(final String name) {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name.toUpperCase());
    }
}
