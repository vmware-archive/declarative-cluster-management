/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Given a view with a group by, extract a view that lists the table of unique groups.
 *
 * Example:
 * Input: select * from T where P1 group by c1, c2, c3 having P2;
 * Output: select c1, c2, c3 from T where P1 group by c1, c2, c3;
 */
class ExtractGroupTable {
    private static final String GROUP_TABLE_PREFIX = "GROUP_TABLE__";

    Optional<CreateView> process(final CreateView view) {
        final String viewName = view.getName().toString();
        final QuerySpecification queryBody = (QuerySpecification) view.getQuery().getQueryBody();
        final List<SelectItem> selectList = new ArrayList<>();
        if (queryBody.getGroupBy().isPresent()) {
            final GroupBy groupBy = queryBody.getGroupBy().get();
            for (final GroupingElement e: groupBy.getGroupingElements()) {
                assert e instanceof SimpleGroupBy;
                final SimpleGroupBy simpleGroupBy = (SimpleGroupBy) e;
                assert simpleGroupBy.getExpressions().size() == 1;
                final Expression column = simpleGroupBy.getExpressions().get(0);
                final SelectItem item = new SingleColumn(column,
                                                 new Identifier(column.toString()
                                                                      .replace(".", "_")));
                selectList.add(item);
            }
        }
        else {
            return Optional.empty();
        }
        assert queryBody.getFrom().isPresent();
        // Next, we make sure that join criteria does not have controllables in them
        final Relation relation = relelationWithControllablesRemoved(queryBody.getFrom().get());
        final Query query = QueryUtil.simpleQuery(QueryUtil.selectList(selectList.toArray(new SelectItem[0])),
                                                  relation,
                                                  queryBody.getWhere(),
                                                  queryBody.getGroupBy(),
                                                  Optional.empty(),
                                                  Optional.empty(),
                                                  Optional.empty());
        return Optional.of(new CreateView(QualifiedName.of(GROUP_TABLE_PREFIX + viewName), query, false));
    }

    private Relation relelationWithControllablesRemoved(final Relation relation) {
        if (relation instanceof Join) {
            final Join join = (Join) relation;
            if (join.getCriteria().isPresent()) {
                final RemoveControllablePredicates removeControllablePredicates = new RemoveControllablePredicates();
                final Expression joinOnExpression = ((JoinOn) join.getCriteria().get()).getExpression();
                final Optional<JoinCriteria> newJoinCriteria = removeControllablePredicates.process(joinOnExpression)
                                                                                           .map(JoinOn::new);
                // If we end up with no join condition at all, then it is equivalent to using an implicit join
                // (e.g., "from T1, T2")
                final Join.Type joinType = newJoinCriteria.isPresent() ?
                                              join.getType() :
                                              Join.Type.IMPLICIT;
                return new Join(joinType, join.getLeft(), join.getRight(), newJoinCriteria);
            }
        }
        return relation;
    }
}
