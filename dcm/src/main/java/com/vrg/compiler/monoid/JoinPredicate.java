/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.compiler.monoid;

public class JoinPredicate extends BinaryOperatorPredicate {
    public JoinPredicate(final BinaryOperatorPredicate predicate) {
        super(predicate.getOperator(), predicate.getLeft(), predicate.getRight());
    }
}
