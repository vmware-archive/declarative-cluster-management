package com.vrg;

import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
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
                assert simpleGroupBy.getColumnExpressions().size() == 1;
                final Expression column = simpleGroupBy.getColumnExpressions().get(0);
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
        final Query query = QueryUtil.simpleQuery(QueryUtil.selectList(selectList.toArray(new SelectItem[0])),
                                                  queryBody.getFrom().get(),
                                                  queryBody.getWhere(),
                                                  queryBody.getGroupBy(),
                                                  Optional.empty(),
                                                  Optional.empty(),
                                                  Optional.empty());
        return Optional.of(new CreateView(QualifiedName.of(GROUP_TABLE_PREFIX + viewName), query, false));
    }
}
