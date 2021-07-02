/*
 * Copyright 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import com.vmware.dcm.Model;
import com.vmware.dcm.ModelException;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SyntaxCheckTest {

    @Test
    public void testUnsupportedCheckSyntax() {
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        conn.execute("create table t1(c1 varchar(100), controllable__dummy integer)");
        final List<String> views = List.of("CREATE VIEW constraint_with_like AS " +
                                           "SELECT * FROM t1 " +
                                           "CHECK c1 LIKE 'node'");
        try {
            Model.build(conn, views);
            fail();
        } catch (final ModelException err) {
            assertTrue(err.getMessage()
                    .contains("---> Unexpected AST type class "));
            assertTrue(err.getMessage()
                    .contains("com.facebook.presto.sql.tree.LikePredicate ((c1 LIKE 'node'))"));
        }
    }

    @Test
    public void testUnsupportedJoinSyntax() {
        final DSLContext conn = DSL.using("jdbc:h2:mem:");
        conn.execute("create table t1(c1 varchar(100), controllable__dummy integer)");
        conn.execute("create table t2(c2 varchar(100))");

        final List<String> views = List.of("CREATE VIEW constraint_with_left_join AS\n" +
                "SELECT * FROM t1 LEFT JOIN t2 on c1 = c2 " +
                "CHECK c1 LIKE 'node'");
        try {
            Model.build(conn, views);
            fail();
        } catch (final ModelException err) {
            assertTrue(err.getMessage()
                    .contains("---> Unexpected AST type class "));
            assertTrue(err.getMessage()
                    .contains("com.facebook.presto.sql.tree.Join " +
                           "(Join{type=LEFT, left=Table{t1}, right=Table{t2}, criteria=Optional[JoinOn{(c1 = c2)}]})"));
        }
    }
}
