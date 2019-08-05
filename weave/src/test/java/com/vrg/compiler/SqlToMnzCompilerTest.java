/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.compiler;

/**
 * Unit test for simple App.
 */
public class SqlToMnzCompilerTest {

//    @Test
//    public void testSymbolExtractor() {
//        final ReferencedSymbols symbols = new ReferencedSymbols();
//        final IRContext context = new IRContext(Collections.emptyMap());
//        final ModelCompiler compiler = new ModelCompiler(context, symbols);
//        final List<String> code = getViewsExample();
//        code.forEach(compiler::extractSymbols);
//        assertEquals(3, symbols.getTables().size());
//        assertEquals(8, symbols.getFields().size());
//        assertEquals(3, symbols.getNonConstraintViews().size());
//        assertEquals(2, symbols.getConstraintViews().size());
//        assertEquals(1, symbols.getSubQueryExpressions().size());
//    }

//    @Test
//    public void testForAllStatementParsing() {
//        final String viewStr = "create view curr_epoch_rows as\n" +
//                "select * from hosts where epoch_id = max(epochs.epoch_id)";
//        final SqlParser parser = new SqlParser();
//        final CreateView view = (CreateView) parser.createStatement(viewStr, new ParsingOptions());
//        final Table hostsTable = table(name("HOSTS"));
//        final IRTable mnzTable = new IRTable(hostsTable);
//        final IRContext context = new IRContext(Collections.singletonMap("HOSTS", mnzTable));
//        final ModelCompiler compiler = new ModelCompiler(context);
//        final ForAllStatement forAllStatement = compiler.parseView(view.getQuery());
//        assertEquals(1, forAllStatement.getMnzTableIterators().size());
//        assertEquals(0, forAllStatement.getJoinCriteria().size());
//        assertTrue(forAllStatement.getWhereClause().isPresent());
//    }

//    @Test
//    public void groupByTest() {
//        final String viewStr = "create view curr_epoch_rows as\n" +
//                "select * from hosts as X where X.epoch_id = max(epochs.epoch_id) group by X.derp";
//        final SqlParser parser = new SqlParser();
//        final CreateView view = (CreateView) parser.createStatement(viewStr, new ParsingOptions());
//        final Table hostsTable = table(name("HOSTS"));
//        final IRTable mnzTable = new IRTable(hostsTable);
//        final IRContext context = new IRContext(Collections.singletonMap("HOSTS", mnzTable));
//        final ModelCompiler compiler = new ModelCompiler(context);
//        System.out.println(compiler.parseViewMonoid(view.getQuery()));
//    }



//    private List<String> getViewsExample() {
//        final String view1 = "create view curr_epoch_rows as\n" +
//                "select * from hosts where epoch_id = (select max(epoch_id) from epochs)";
//        final String view2 = "create view old_epoch_rows as\n" +
//                "select * from hosts where epoch_id != max(epoch_id)";
//        final String view3 = "create view constraint_unresponsive_hosts_in_segments as\n" +
//                "select * from curr_epoch_rows \n" +
//                "       where failure_state = 'UNRESPONSIVE' and controllable_in_segment != 0";
//        final String view4 = "create view constraint_in_segments_old as\n" +
//                "select * from hosts join old_epoch_rows on hosts.epochId = old_epochs_rows.epochId \n" +
//                "       where in_segment != controllable_in_segment";
//        final String view5 = "create view min_roles as\n "
//                + "select count(*) from curr_epoch_rows \n"
//                + "having sum(is_layout) < 2 " +
//                "      or sum(is_segment) < 2 " +
//                "      or sum(is_layout) < 2";
//        return Lists.newArrayList(view1, view2, view3, view4, view5);
//    }
}