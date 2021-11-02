package com.vmware.dcm;

import com.vmware.dcm.compiler.IRContext;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;


import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class AutoScopeTest {
    /*
     * Test if Scope only keeps the least loaded nodes when no constraints are present.
     */
    @Test
    public void testSetup() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> constraints = Policies.getInitialPlacementPolicies();
        final Model model = Model.build(conn, constraints);
        final IRContext irContext = model.getIrContext();
        final Map<String, String> augmViews = AutoScope.augmentedViews(constraints, irContext, 20);
        assertEquals(2, augmViews.size());

        final List<String> statements = AutoScope.getViewStatements(augmViews);
        statements.forEach(conn::execute);
        // finish without error
    }

}
