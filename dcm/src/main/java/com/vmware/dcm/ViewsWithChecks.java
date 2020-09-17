/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * A data carrier for views and their corresponding (optional) check expression.
 *
 * This allows us to avoid needing a new parser for CreateView statements.
 */
public class ViewsWithChecks {
    private static final SqlParser PARSER = new SqlParser();
    private static final ParsingOptions OPTIONS = ParsingOptions.builder().build();
    private final CreateView createView;
    @Nullable private final Expression checkExpression;

    private ViewsWithChecks(final CreateView createView) {
        this.createView = createView;
        this.checkExpression = null;
    }

    private ViewsWithChecks(final CreateView createView, final Expression checkExpression) {
        this.createView = createView;
        this.checkExpression = checkExpression;
    }

    public CreateView getCreateView() {
        return createView;
    }

    public Optional<Expression> getCheckExpression() {
        return Optional.ofNullable(checkExpression);
    }

    /**
     * Parses a view with or without a check statement into a corresponding
     * CreateView and optional check expression type
     *
     * @param view a string representing a view with or without a check statement
     * @return a ViewsWithChecks instance
     */
    public static ViewsWithChecks fromString(final String view) {
        final List<String> splitQuery = Splitter.onPattern("(?i)\\scheck\\s").splitToList(view);
        Preconditions.checkState(splitQuery.size() <= 2,
                "Malformed query (splitting by 'check' yielded an array with more than 2 parts): " + view);
        final String relationWithoutConstraint = splitQuery.get(0);
        final CreateView createViewStatement = (CreateView) PARSER.createStatement(relationWithoutConstraint, OPTIONS);

        if (splitQuery.size() == 2) {
            final String checkExpressionStr = splitQuery.get(1);
            final Expression checkExpression = PARSER.createExpression(checkExpressionStr, OPTIONS);
            return new ViewsWithChecks(createViewStatement, checkExpression);
        } else {
            return new ViewsWithChecks(createViewStatement);
        }
    }
}