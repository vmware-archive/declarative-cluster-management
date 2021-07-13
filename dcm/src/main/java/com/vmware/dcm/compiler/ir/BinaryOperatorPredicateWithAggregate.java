/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

public class BinaryOperatorPredicateWithAggregate extends BinaryOperatorPredicate {
    public BinaryOperatorPredicateWithAggregate(final Operator operator, final Expr left, final Expr right) {
        super(operator, left, right);
    }
}