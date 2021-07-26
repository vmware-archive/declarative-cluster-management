/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Optional;

public class SqlCreateConstraint extends SqlCreate {
    public static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE CONSTRAINT", SqlKind.OTHER_DDL);
    private final SqlIdentifier name;
    private final SqlSelect query;
    private final SqlNode constraint;
    private final Type type;

    /** Creates a SqlCreateConstraint. */
    public SqlCreateConstraint(final SqlParserPos pos, final SqlIdentifier name, final SqlNode query,
                               final String type, final SqlNode constraint) {
        super(OPERATOR, pos, false, false);
        this.name = name;
        this.query = (SqlSelect) query;
        this.constraint = constraint;
        this.type = Type.valueOf(type);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlSelect getQuery() {
        return query;
    }

    public Optional<SqlNode> getConstraint() {
        return Optional.ofNullable(constraint);
    }

    public Type getType() {
        return type;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return constraint == null ? ImmutableList.of(name, query) : ImmutableList.of(name, query, constraint);
    }

    @Override
    public String toString() {
        return "SqlCreateConstraint{" +
                "name=" + name +
                '}';
    }

    @Override
    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("CONSTRAINT");
        getName().unparse(writer, leftPrec, rightPrec);
        getQuery().unparse(writer, leftPrec, rightPrec);
        getConstraint().ifPresent(e -> e.unparse(writer, leftPrec, rightPrec));
        super.unparse(writer, leftPrec, rightPrec);
    }

    public enum Type {
        HARD_CONSTRAINT,
        OBJECTIVE,
        INTERMEDIATE_VIEW
    }
}
