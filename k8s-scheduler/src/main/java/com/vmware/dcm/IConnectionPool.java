package com.vmware.dcm;

import com.google.common.annotations.VisibleForTesting;
import org.jooq.DSLContext;

interface IConnectionPool {
    DSLContext getConnectionToDb();

    DSLContext getDataConnectionToDb();

    @VisibleForTesting
    default void refresh() {
        throw new RuntimeException("Don't know how to refresh for IConnectionPool..?");
    }
}
