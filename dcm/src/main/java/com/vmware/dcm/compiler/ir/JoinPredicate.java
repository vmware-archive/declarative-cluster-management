/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.ir;

public class JoinPredicate extends BinaryOperatorPredicate {
    public JoinPredicate(final BinaryOperatorPredicate predicate) {
        super(predicate.getOperator(), predicate.getLeft(), predicate.getRight());
    }
}
