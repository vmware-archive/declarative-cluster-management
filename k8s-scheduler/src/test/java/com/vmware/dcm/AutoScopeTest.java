package com.vmware.dcm;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;


import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class AutoScopeTest {
    /*
     * Test if Scope only keeps the least loaded nodes when no constraints are present.
     */
    @Test
    public void testSetup() {
        List<String> constraints = Policies.getInitialPlacementPolicies();
        List<String> augmViews = AutoScope.augmentedViews(DBViews.getSchema(), constraints);
        assertEquals(1, augmViews.size());

        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();

        augmViews.forEach(conn::execute);
        var model = Model.build(conn, constraints);
    }

}
