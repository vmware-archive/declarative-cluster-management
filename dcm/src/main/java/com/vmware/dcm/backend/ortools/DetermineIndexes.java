/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.JoinPredicate;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.SimpleVisitor;
import com.vmware.dcm.compiler.ir.TableRowGenerator;
import com.vmware.dcm.compiler.ir.VoidType;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Recursively scans for joins to identify accesses that can use indexes.
 */
class DetermineIndexes {
    final CheckIndex visitor = new CheckIndex();

    void apply(final ListComprehension comprehension) {
        visitor.visit(comprehension);
    }

    Set<IndexDescription> indexes() {
        return visitor.indexes;
    }

    static class CheckIndex extends SimpleVisitor {
        final Set<IndexDescription> indexes = new HashSet<>();

        @Override
        protected VoidType visitListComprehension(final ListComprehension node, final VoidType context) {
            final ListComprehension inner = node instanceof GroupByComprehension ?
                    ((GroupByComprehension) node).getComprehension() : node;
            final List<TableRowGenerator> tableRowGenerators = inner.getQualifiers().stream()
                    .filter(e -> e instanceof TableRowGenerator)
                    .map(e -> (TableRowGenerator) e).collect(Collectors.toList());
            final List<JoinPredicate> joinPredicates = inner.getQualifiers().stream()
                    .filter(e -> e instanceof JoinPredicate)
                    .map(e -> (JoinPredicate) e).collect(Collectors.toList());

            // For now, we always use a scan for the first Table being iterated over.
            tableRowGenerators.subList(1, tableRowGenerators.size())
                    .forEach(tr -> {
                        // We might be able to use an index here, look for equality based accesses
                        // across the join predicates
                        for (final BinaryOperatorPredicate binaryOp : joinPredicates) {
                            if (binaryOp.getOperator().equals(BinaryOperatorPredicate.Operator.EQUAL)) {
                                final ColumnIdentifier left = (ColumnIdentifier) binaryOp.getLeft();
                                final ColumnIdentifier right = (ColumnIdentifier) binaryOp.getRight();
                                if (right.getTableName().equals(tr.getTable().getName())) {
                                    indexes.add(new IndexDescription(tr, right));
                                } else if (left.getTableName().equals(tr.getTable().getName())) {
                                    indexes.add(new IndexDescription(tr, left));
                                }
                            }
                        }
                    });
            return super.visitListComprehension(node, context);
        }
    }

    static class IndexDescription {
        final TableRowGenerator relation;
        final ColumnIdentifier columnBeingAccessed;

        IndexDescription(final TableRowGenerator relation, final ColumnIdentifier columnBeingAccessed) {
            this.relation = relation;
            this.columnBeingAccessed = columnBeingAccessed;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IndexDescription)) {
                return false;
            }
            final IndexDescription that = (IndexDescription) o;
            return Objects.equals(relation.getTable().getAliasedName(), that.relation.getTable().getAliasedName())
                && Objects.equals(columnBeingAccessed.getField().getName(),
                                  that.columnBeingAccessed.getField().getName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(relation.getTable().getAliasedName(), columnBeingAccessed.getField().getName());
        }

        @Override
        public String toString() {
            return "IndexDescription{" +
                    "relation=" + relation +
                    ", columnBeingAccessed=" + columnBeingAccessed +
                    '}';
        }
    }
}
