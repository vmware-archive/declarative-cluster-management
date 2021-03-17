/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.FunctionCall;
import com.vmware.dcm.compiler.ir.SimpleVisitor;
import com.vmware.dcm.compiler.ir.VoidType;

class ContainsFunctionCall extends SimpleVisitor {
    boolean found = false;

    @Override
    protected VoidType visitFunctionCall(final FunctionCall node, final VoidType context) {
        found = true;
        return super.visitFunctionCall(node, context);
    }

    boolean getFound() {
        return found;
    }
}
