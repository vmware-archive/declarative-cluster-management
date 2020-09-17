/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.monoid;

public class BinaryOperatorPredicateWithAggregate extends BinaryOperatorPredicate {
    public BinaryOperatorPredicateWithAggregate(final Operator operator, final Expr left, final Expr right) {
        super(operator, left, right);
    }

    public BinaryOperatorPredicateWithAggregate(final BinaryOperatorPredicate node) {
        super(node.getOperator(), node.getLeft(), node.getRight());
    }
}
