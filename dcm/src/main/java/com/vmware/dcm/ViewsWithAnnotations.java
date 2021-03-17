/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
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
 * A data carrier for views and their corresponding optional check expression or maximize expression.
 *
 * This allows us to avoid needing a new parser for CreateView statements.
 */
public class ViewsWithAnnotations {
    private static final SqlParser PARSER = new SqlParser();
    private static final ParsingOptions OPTIONS = ParsingOptions.builder().build();
    private final CreateView createView;
    @Nullable private final Expression checkExpression;
    private final boolean isObjective;

    private ViewsWithAnnotations(final CreateView createView) {
        this.createView = createView;
        this.checkExpression = null;
        this.isObjective = false;
    }

    private ViewsWithAnnotations(final CreateView createView, final Expression checkExpression) {
        this.createView = createView;
        this.checkExpression = checkExpression;
        this.isObjective = false;
    }

    private ViewsWithAnnotations(final CreateView createView, final boolean isObjective) {
        this.createView = createView;
        this.checkExpression = null;
        this.isObjective = isObjective;
    }

    public CreateView getCreateView() {
        return createView;
    }

    public Optional<Expression> getCheckExpression() {
        return Optional.ofNullable(checkExpression);
    }

    public boolean isObjective() {
        return isObjective;
    }

    /**
     * Parses a view with or without a check statement into a corresponding
     * CreateView and optional check expression type
     *
     * @param view a string representing a view with or without a check statement
     * @return a ViewsWithChecks instance
     */
    public static ViewsWithAnnotations fromString(final String view) {
        final List<String> splitQueryOnCheck = Splitter.onPattern("(?i)\\scheck\\s")
                                                       .trimResults()
                                                       .omitEmptyStrings().splitToList(view);
        Preconditions.checkState(splitQueryOnCheck.size() <= 2,
                "Malformed query (splitting by 'check' yielded an array with more than 2 parts): " + view);
        if (splitQueryOnCheck.size() == 2) {
            Preconditions.checkState(!view.contains("maximize"),
                    "A query cannot have both a check clause and a maximize annotation: " + view);
            final String relationWithoutConstraint = splitQueryOnCheck.get(0);
            final CreateView createViewStatement = (CreateView) PARSER.createStatement(relationWithoutConstraint,
                                                                                       OPTIONS);
            final String checkExpressionStr = splitQueryOnCheck.get(1);
            final Expression checkExpression = PARSER.createExpression(checkExpressionStr, OPTIONS);
            return new ViewsWithAnnotations(createViewStatement, checkExpression);
        }
        if (view.contains("maximize")) {
            final List<String> splitOnMaximize = Splitter.on("maximize")
                                                         .trimResults()
                                                         .omitEmptyStrings()
                                                         .splitToList(view.trim());
            Preconditions.checkState(splitOnMaximize.size() >= 1,
                    "Malformed query (splitting by 'maximize' yielded an array with fewer than one part): "
                            + view);
            final String expression = splitOnMaximize.get(0);
            final CreateView createViewStatement = (CreateView) PARSER.createStatement(expression, OPTIONS);
            return new ViewsWithAnnotations(createViewStatement, true);
        } else {
            final CreateView createViewStatement = (CreateView) PARSER.createStatement(view, OPTIONS);
            return new ViewsWithAnnotations(createViewStatement);
        }
    }
}