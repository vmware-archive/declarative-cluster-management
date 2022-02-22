/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.ModelException;
import com.vmware.dcm.compiler.IRTable;
import com.vmware.dcm.compiler.ir.BinaryOperatorPredicate;
import com.vmware.dcm.compiler.ir.ColumnIdentifier;
import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.ListComprehension;
import com.vmware.dcm.compiler.ir.SimpleVisitor;
import com.vmware.dcm.compiler.ir.TableRowGenerator;
import com.vmware.dcm.compiler.ir.VoidType;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
            final List<BinaryOperatorPredicate> joinPredicates = inner.getQualifiers().stream()
                    .filter(e -> e instanceof BinaryOperatorPredicate)
                    .map(e -> (BinaryOperatorPredicate) e).collect(Collectors.toList());

            // For now, we always use a scan for the first Table being iterated over.
            tableRowGenerators.forEach(tr -> {
                        // We might be able to use an index here, look for equality based accesses
                        // across the join predicates
                        for (final BinaryOperatorPredicate binaryOp : joinPredicates) {
                            maybeIndex(binaryOp, tr.getTable())
                                    .ifPresent(indexes::add);
                        }
                    });
            return super.visitListComprehension(node, context);
        }
    }

    static Optional<IndexDescription> maybeIndex(final BinaryOperatorPredicate op,
                                                 final IRTable innerTable) {
        if (!(op.getLeft() instanceof ColumnIdentifier && op.getRight() instanceof ColumnIdentifier)) {
            return Optional.empty();
        }
        final ColumnIdentifier left = (ColumnIdentifier) op.getLeft();
        final ColumnIdentifier right = (ColumnIdentifier) op.getRight();
        final String leftTableNameOrAlias = left.getField().getIRTable().getAliasedName();
        final String rightTableNameOrAlias = right.getField().getIRTable().getAliasedName();
        final String innerTableNameOrAlias = innerTable.getAliasedName();
        if (rightTableNameOrAlias.equals(innerTableNameOrAlias)) {
            return Optional.of(new IndexDescription(innerTable, right));
        } else if (leftTableNameOrAlias.equals(innerTableNameOrAlias)) {
            return Optional.of(new IndexDescription(innerTable, left));
        }
        return Optional.empty();
    }

    static class IndexedAccess {
        final ColumnIdentifier indexedColumn;
        final ColumnIdentifier scanColumn;

        IndexedAccess(final ColumnIdentifier indexedColumn, final ColumnIdentifier scanColumn) {
            this.indexedColumn = indexedColumn;
            this.scanColumn = scanColumn;
        }
    }

    static class IndexDescription {
        final IRTable relation;
        final ColumnIdentifier columnBeingAccessed;

        IndexDescription(final IRTable relation, final ColumnIdentifier columnBeingAccessed) {
            this.relation = relation;
            this.columnBeingAccessed = columnBeingAccessed;
        }

        public IndexedAccess toIndexedAccess(final BinaryOperatorPredicate op) {
            final ColumnIdentifier left = (ColumnIdentifier) op.getLeft();
            final ColumnIdentifier right = (ColumnIdentifier) op.getRight();
            if (left.getField().getIRTable().getAliasedName().equals(relation.getAliasedName())) {
                return new IndexedAccess(left, right);
            } else if (right.getField().getIRTable().getAliasedName().equals(relation.getAliasedName())) {
                return new IndexedAccess(right, left);
            }
            throw new ModelException("Unreachable");
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
            return Objects.equals(relation.getAliasedName(), that.relation.getAliasedName())
                && Objects.equals(columnBeingAccessed.getField().getName(),
                                  that.columnBeingAccessed.getField().getName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(relation.getAliasedName(), columnBeingAccessed.getField().getName());
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
