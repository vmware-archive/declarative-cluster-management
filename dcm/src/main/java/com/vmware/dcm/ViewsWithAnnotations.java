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
import java.util.Locale;
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
    private final boolean isObjective;
    private final String inputView;
    @Nullable private final String inputConstraint;
    @Nullable private final Expression constraint;

    private ViewsWithAnnotations(final CreateView createView, final String inputView) {
        this.createView = createView;
        this.constraint = null;
        this.inputConstraint = null;
        this.isObjective = false;
        this.inputView = inputView;
    }

    private ViewsWithAnnotations(final CreateView createView, final Expression constraint, final boolean isObjective,
                                 final String inputView, final String inputConstraint) {
        this.createView = createView;
        this.isObjective = isObjective;
        this.constraint = constraint;
        this.inputView = inputView;
        this.inputConstraint = inputConstraint;
    }

    public String getInputView() {
        return inputView;
    }

    public String getInputConstraint() {
        return inputConstraint;
    }

    public CreateView getCreateView() {
        return createView;
    }

    public Optional<Expression> getCheckExpression() {
        return !isObjective ? Optional.ofNullable(constraint) : Optional.empty();
    }

    public Optional<Expression> getMaximizeExpression() {
        return isObjective ? Optional.ofNullable(constraint) : Optional.empty();
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
        final List<String> splitQueryOnMaximize = Splitter.onPattern("(?i)\\smaximize\\s")
                                                       .trimResults()
                                                       .omitEmptyStrings().splitToList(view);
        if (splitQueryOnCheck.size() == 2) {
            Preconditions.checkState(!view.toLowerCase(Locale.ROOT).contains("maximize"),
                    "A query cannot have both a check clause and a maximize annotation: " + view);
            final String relationWithoutConstraint = splitQueryOnCheck.get(0);
            final CreateView createViewStatement = (CreateView) PARSER.createStatement(relationWithoutConstraint,
                                                                                       OPTIONS);
            final String checkExpressionStr = splitQueryOnCheck.get(1);
            final Expression checkExpression = PARSER.createExpression(checkExpressionStr, OPTIONS);
            return new ViewsWithAnnotations(createViewStatement, checkExpression, false,
                                            relationWithoutConstraint, checkExpressionStr);
        }
        else if (splitQueryOnMaximize.size() == 2) {
            Preconditions.checkState(!view.toLowerCase(Locale.ROOT).contains("check"),
                    "A query cannot have both a check clause and a maximize annotation: " + view);
            final String relationWithoutConstraint = splitQueryOnMaximize.get(0);
            final CreateView createViewStatement = (CreateView) PARSER.createStatement(relationWithoutConstraint,
                    OPTIONS);
            final String maximizeExpressionStr = splitQueryOnMaximize.get(1);
            final Expression maximizeExpression = PARSER.createExpression(maximizeExpressionStr, OPTIONS);
            return new ViewsWithAnnotations(createViewStatement, maximizeExpression, true,
                                            relationWithoutConstraint, maximizeExpressionStr);
        } else {
            Preconditions.checkState(!view.toLowerCase(Locale.ROOT).contains("maximize") &&
                                     !view.toLowerCase(Locale.ROOT).contains("check"),
                    "Unexpected query: non-constraint view cannot have a check clause or a maximize " +
                                "annotation: " + view);
            final CreateView createViewStatement = (CreateView) PARSER.createStatement(view, OPTIONS);
            return new ViewsWithAnnotations(createViewStatement, view);
        }
    }
}