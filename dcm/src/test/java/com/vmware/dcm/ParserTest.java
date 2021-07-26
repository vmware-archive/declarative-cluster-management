/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.generated.parser.DcmSqlParserImpl;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.OTHER_DDL;
import static org.apache.calcite.sql.SqlKind.SELECT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParserTest {

    @Test
    public void testMaximize() throws SqlParseException {
        final SqlParser.Config config = SqlParser.config().withParserFactory(DcmSqlParserImpl.FACTORY);
        final SqlParser parser =
                SqlParser.create("create constraint xyz as select * from t1 maximize t1.c1 = 10", config);
        final SqlNode sqlNode = parser.parseStmt();
        final SqlCreateConstraint constraint = (SqlCreateConstraint) sqlNode;
        assertEquals("XYZ", constraint.getName().getSimple());
        assertEquals(OTHER_DDL, constraint.getOperator().getKind());
        assertEquals(SELECT, constraint.getQuery().getKind());
        assertEquals(EQUALS, constraint.getConstraint().get().getKind());
        assertEquals(SqlCreateConstraint.Type.OBJECTIVE, constraint.getType());
    }

    @Test
    public void testCheck() throws SqlParseException {
        final SqlParser.Config config = SqlParser.config().withParserFactory(DcmSqlParserImpl.FACTORY);
        final SqlParser parser =
                SqlParser.create("create constraint xyz as select * from t1 check t1.c1 = 10", config);
        final SqlNode sqlNode = parser.parseStmt();
        final SqlCreateConstraint constraint = (SqlCreateConstraint) sqlNode;
        assertEquals("XYZ", constraint.getName().getSimple());
        assertEquals(OTHER_DDL, constraint.getOperator().getKind());
        assertEquals(SELECT, constraint.getQuery().getKind());
        assertEquals(EQUALS, constraint.getConstraint().get().getKind());
        assertEquals(SqlCreateConstraint.Type.HARD_CONSTRAINT, constraint.getType());
    }

    @Test
    public void testIntermediateView() throws SqlParseException {
        final SqlParser.Config config = SqlParser.config().withParserFactory(DcmSqlParserImpl.FACTORY);
        final SqlParser parser =
                SqlParser.create("create constraint xyz as select * from t1", config);
        final SqlNode sqlNode = parser.parseStmt();
        final SqlCreateConstraint constraint = (SqlCreateConstraint) sqlNode;
        assertEquals("XYZ", constraint.getName().getSimple());
        assertEquals(OTHER_DDL, constraint.getOperator().getKind());
        assertEquals(SELECT, constraint.getQuery().getKind());
        assertEquals(SqlCreateConstraint.Type.INTERMEDIATE_VIEW, constraint.getType());
    }
}
