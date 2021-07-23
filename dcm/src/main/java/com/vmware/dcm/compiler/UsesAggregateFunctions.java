/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.vmware.dcm.compiler.ir.FunctionCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import static org.apache.calcite.sql.SqlKind.OTHER_FUNCTION;

public class UsesAggregateFunctions extends SqlBasicVisitor<Void> {
    private boolean found = false;

    @Override
    public Void visit(final SqlCall call) {
        if (call.getKind() == OTHER_FUNCTION) {
            final FunctionCall.Function function =
                    FunctionCall.Function.valueOf(call.getOperator().getName().toUpperCase());
            switch (function) {
                case COUNT:
                case SUM:
                case MIN:
                case MAX:
                case ANY:
                case ALL:
                case ALL_DIFFERENT:
                case ALL_EQUAL:
                case INCREASING:
                case CAPACITY_CONSTRAINT:
                    found = true;
                    break;
                default:
                    found = false;
            }
        }
        if (call instanceof SqlSelect) {
            return null;
        }
        return super.visit(call);
    }

    public boolean isFound() {
        return found;
    }
}
