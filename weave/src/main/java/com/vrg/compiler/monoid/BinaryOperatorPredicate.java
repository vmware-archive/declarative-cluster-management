/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg.compiler.monoid;

import javax.annotation.Nullable;

public class BinaryOperatorPredicate extends Qualifier {
    private final String operator;
    private final Expr left;
    private final Expr right;

    public BinaryOperatorPredicate(final String operator, final Expr left, final Expr right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        return left + " " + operator + " " + right;
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        visitor.visitBinaryOperatorPredicate(this, context);
        return null;
    }

    public Expr getLeft() {
        return left;
    }

    public String getOperator() {
        return operator;
    }

    public Expr getRight() {
        return right;
    }
}
