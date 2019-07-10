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

public class MonoidVisitor {

    public void visit(final Expr expr) {
        expr.acceptVisitor(this);
    }

    protected void visitHead(final Head node) {
    }

    protected void visitTableRowGenerator(final TableRowGenerator node) {
    }

    protected void visitMonoidComprehension(final MonoidComprehension node) {
        node.getHead().acceptVisitor(this);
        for (final Qualifier qualifier: node.getQualifiers()) {
            qualifier.acceptVisitor(this);
        }
    }

    protected void visitBinaryOperatorPredicate(final BinaryOperatorPredicate node) {
        node.getLeft().acceptVisitor(this);
        node.getRight().acceptVisitor(this);
    }

    protected void visitGroupByComprehension(final GroupByComprehension node) {
        node.getComprehension().acceptVisitor(this);
        node.getGroupByQualifier().acceptVisitor(this);
    }

    protected void visitGroupByQualifier(final GroupByQualifier node) {
    }

    protected void visitMonoidLiteral(final MonoidLiteral node) {
    }

    protected void visitMonoidFunction(final MonoidFunction node) {
        node.getArgument().acceptVisitor(this);
    }

    protected void visitQualifier(final Qualifier node) {
    }

    protected void visitColumnIdentifier(final ColumnIdentifier node) {
    }

    protected void visitExistsPredicate(final ExistsPredicate node) {
        node.getArgument().acceptVisitor(this);
    }
}