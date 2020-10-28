/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.monoid.MonoidFunction;
import com.vmware.dcm.compiler.monoid.SimpleVisitor;
import com.vmware.dcm.compiler.monoid.VoidType;

class ContainsMonoidFunction extends SimpleVisitor {
    boolean found = false;

    @Override
    protected VoidType visitMonoidFunction(final MonoidFunction node, final VoidType context) {
        found = true;
        return super.visitMonoidFunction(node, context);
    }

    boolean getFound() {
        return found;
    }
}
